package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/luigi/kube-doctor/internal/api"
	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/llm"
	"github.com/luigi/kube-doctor/internal/mcp"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
)

func setupTestServer() (*api.Server, *k8s.Client, *mcp.K8sMCPClient) {
	ragEngine := rag.NewEngine("../../knowledge/k8s_1_35_ocp_4_22.json")
	k8sClient := k8s.NewClient()
	k8sMCPClient := mcp.NewK8sMCPClient()
	gitopsMgr := gitops.NewManager(k8sClient)
	remEngine := remediation.NewEngine(k8sClient, gitopsMgr, k8sMCPClient)
	llmEngine := llm.NewEngine(ragEngine, remEngine)
	mcpServer := mcp.NewServer(ragEngine, remEngine, gitopsMgr, k8sMCPClient)
	srv := api.NewServer(llmEngine, remEngine, gitopsMgr, k8sClient, mcpServer)
	return srv, k8sClient, k8sMCPClient
}

func TestStatefulSetOOMWithinQuotaAutoRemediatesViaK8sMCPServer(t *testing.T) {
	srv, k8sClient, k8sMCP := setupTestServer()

	ev := types.DiagnosticEvent{
		ClusterVersion:   "v1.35.2",
		OpenShiftVersion: "4.22.1",
		Namespace:        "payments",
		PodName:          "redis-cluster-0",
		ContainerName:    "redis",
		OwnerKind:        "StatefulSet",
		OwnerName:        "redis-cluster",
		ExitCode:         137,
		Reason:           "OOMKilled",
		Message:          "Container redis OOMKilled at 512Mi limit",
		CurrentMemoryReq: "256Mi",
		CurrentMemoryLim: "512Mi",
		QuotaName:        "payments-quota",
		QuotaMemoryLimit: "8Gi",
		QuotaMemoryUsed:  "2Gi",
	}
	body, _ := json.Marshal(ev)
	req := httptest.NewRequest(http.MethodPost, "/v1/diagnostics/ingest", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rec.Code, rec.Body.String())
	}

	var assessment types.TriageAssessment
	if err := json.Unmarshal(rec.Body.Bytes(), &assessment); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if assessment.IssueType != types.IssuePodOOMKilled {
		t.Fatalf("expected IssuePodOOMKilled, got %s", assessment.IssueType)
	}
	if len(assessment.PlannedActions) != 1 || assessment.PlannedActions[0].Status != "EXECUTED" {
		t.Fatalf("expected 1 EXECUTED action, got %+v", assessment.PlannedActions)
	}
	if len(k8sClient.AuditEntries()) < 2 {
		t.Fatalf("expected StatefulSet patch + K8s 1.35 pods/resize patch in audit log, got %d", len(k8sClient.AuditEntries()))
	}
	if len(k8sMCP.CallRecords()) < 4 {
		t.Fatalf("expected >=4 kubernetes-mcp-server tool calls, got %d", len(k8sMCP.CallRecords()))
	}
}

func TestNamespaceQuotaExceededRequiresApprovalThenExecutes(t *testing.T) {
	srv, _, _ := setupTestServer()

	ev := types.DiagnosticEvent{
		ClusterVersion:   "v1.35.2",
		OpenShiftVersion: "4.22.1",
		Namespace:        "analytics",
		PodName:          "clickhouse-0",
		ContainerName:    "clickhouse",
		OwnerKind:        "StatefulSet",
		OwnerName:        "clickhouse",
		ExitCode:         137,
		Reason:           "OOMKilled",
		Message:          "Container clickhouse OOMKilled",
		CurrentMemoryReq: "1Gi",
		CurrentMemoryLim: "2Gi",
		QuotaName:        "analytics-quota",
		QuotaMemoryLimit: "4Gi",
		QuotaMemoryUsed:  "3584Mi",
	}
	body, _ := json.Marshal(ev)
	req := httptest.NewRequest(http.MethodPost, "/v1/diagnostics/ingest", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var assessment types.TriageAssessment
	_ = json.Unmarshal(rec.Body.Bytes(), &assessment)

	if assessment.IssueType != types.IssueNamespaceQuotaExceed {
		t.Fatalf("expected IssueNamespaceQuotaExceed, got %s", assessment.IssueType)
	}
	if assessment.PlannedActions[0].Status != "PENDING_APPROVAL" {
		t.Fatalf("expected PENDING_APPROVAL, got %s", assessment.PlannedActions[0].Status)
	}
	approvalID := assessment.PlannedActions[0].ApprovalID
	if approvalID == "" {
		t.Fatalf("expected non-empty ApprovalID")
	}

	appReq := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+approvalID+"/approve", bytes.NewReader([]byte(`{"approvedBy":"sre-lead"}`)))
	appRec := httptest.NewRecorder()
	srv.ServeHTTP(appRec, appReq)

	if appRec.Code != http.StatusOK {
		t.Fatalf("approve returned %d: %s", appRec.Code, appRec.Body.String())
	}

	var approved types.ApprovalRequest
	_ = json.Unmarshal(appRec.Body.Bytes(), &approved)
	if approved.Status != "APPROVED" || approved.Action.Status != "EXECUTED" {
		t.Fatalf("expected APPROVED and EXECUTED, got %+v", approved)
	}
	if len(approved.FollowUpActions) != 1 || approved.FollowUpActions[0].Status != "EXECUTED" {
		t.Fatalf("expected chained StatefulSet follow-up action to be EXECUTED after quota approval, got %+v", approved.FollowUpActions)
	}
}

func TestOpenShiftMachineMissingRequiresApprovalToAutoscale(t *testing.T) {
	srv, _, k8sMCP := setupTestServer()

	ev := types.DiagnosticEvent{
		ClusterVersion:   "v1.35.2",
		OpenShiftVersion: "4.22.1",
		Namespace:        "openshift-machine-api",
		MachineSetName:   "ocp422-worker-us-east-1b",
		ExpectedMachines: 4,
		ReadyMachines:    3,
		Reason:           "MachineFailed",
		Message:          "Machine ocp422-worker-us-east-1b-xyz99 in Failed state",
	}
	body, _ := json.Marshal(ev)
	req := httptest.NewRequest(http.MethodPost, "/v1/diagnostics/ingest", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var assessment types.TriageAssessment
	_ = json.Unmarshal(rec.Body.Bytes(), &assessment)

	if assessment.IssueType != types.IssueMachineMissing {
		t.Fatalf("expected IssueMachineMissing, got %s", assessment.IssueType)
	}
	approvalID := assessment.PlannedActions[0].ApprovalID

	appReq := httptest.NewRequest(http.MethodPost, "/v1/approvals/"+approvalID+"/approve", bytes.NewReader([]byte(`{"approvedBy":"ocp-admin"}`)))
	appRec := httptest.NewRecorder()
	srv.ServeHTTP(appRec, appReq)

	foundScale := false
	for _, call := range k8sMCP.CallRecords() {
		if call.ToolName == mcp.ToolResourcesScale {
			foundScale = true
		}
	}
	if !foundScale {
		t.Fatalf("expected resources_scale to be invoked on containers/kubernetes-mcp-server upon approval")
	}
}

func TestExpandedKubernetesAndOpenShiftScenarios(t *testing.T) {
	srv, _, _ := setupTestServer()

	cases := []struct {
		name           string
		ev             types.DiagnosticEvent
		expectedIssue  types.IssueType
		expectedPolicy types.ActionPolicy
	}{
		{
			name:           "PVC Filesystem Full Auto-Expansion",
			ev:             types.DiagnosticEvent{Namespace: "db", PVCName: "data-pg-0", CurrentStorage: "20Gi", Reason: "PVCFull"},
			expectedIssue:  types.IssuePVCCapacityExhausted,
			expectedPolicy: types.PolicyAutoRemediate,
		},
		{
			name:           "Node NotReady Out-Of-Service Taint Approval",
			ev:             types.DiagnosticEvent{NodeName: "worker-04", Reason: "NodeNotReady", Message: "Kubelet stopped posting node status"},
			expectedIssue:  types.IssueNodeNotReady,
			expectedPolicy: types.PolicyApprovalRequired,
		},
		{
			name:           "K8s 1.35 DRA GPU ResourceClaim Allocation Failure",
			ev:             types.DiagnosticEvent{Namespace: "ai", OwnerName: "gpu-claim", Reason: "DRAAllocationFailed", Message: "ResourceClaim allocation failed"},
			expectedIssue:  types.IssueDRAAllocationFailure,
			expectedPolicy: types.PolicyApprovalRequired,
		},
		{
			name:           "PDB Blocking Node Drain",
			ev:             types.DiagnosticEvent{Namespace: "prod", PDBName: "checkout-pdb", Reason: "PDBBlockingDrain", Message: "disruptionsAllowed=0"},
			expectedIssue:  types.IssuePDBBlockingDrain,
			expectedPolicy: types.PolicyApprovalRequired,
		},
		{
			name:           "Native Sidecar Container Probe Failure",
			ev:             types.DiagnosticEvent{Namespace: "mesh", OwnerName: "orders", Reason: "SidecarUnhealthy", Message: "Sidecar istio-proxy failed startupProbe"},
			expectedIssue:  types.IssueSidecarContainerFailure,
			expectedPolicy: types.PolicyAutoRemediate,
		},
		{
			name:           "OpenShift 4.22 ClusterOperator Degraded",
			ev:             types.DiagnosticEvent{ClusterOperator: "authentication", Reason: "ClusterOperatorDegraded", Message: "Degraded=True"},
			expectedIssue:  types.IssueClusterOperatorDegrad,
			expectedPolicy: types.PolicyApprovalRequired,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(tc.ev)
			req := httptest.NewRequest(http.MethodPost, "/v1/diagnostics/ingest", bytes.NewReader(body))
			rec := httptest.NewRecorder()
			srv.ServeHTTP(rec, req)

			var assessment types.TriageAssessment
			if err := json.Unmarshal(rec.Body.Bytes(), &assessment); err != nil {
				t.Fatalf("unmarshal failed: %v", err)
			}
			if assessment.IssueType != tc.expectedIssue {
				t.Fatalf("expected %s, got %s", tc.expectedIssue, assessment.IssueType)
			}
			if assessment.PlannedActions[0].Policy != tc.expectedPolicy {
				t.Fatalf("expected policy %s, got %s", tc.expectedPolicy, assessment.PlannedActions[0].Policy)
			}
		})
	}
}

func TestCopilotAgentWithContainersKubernetesMCPServerTools(t *testing.T) {
	srv, _, _ := setupTestServer()

	chatReq := types.ChatCompletionRequest{
		Model: llm.DefaultModelName,
		Messages: []types.ChatMessage{
			{
				Role:    "user",
				Content: "Our StatefulSet pod postgres-ha-0 is OOMKilled. Please inspect it via kubernetes-mcp-server and search web for latest errata on OpenShift 4.22 CRI-O cgroup v2.",
			},
		},
		Tools: []types.ChatTool{
			{
				Type: "function",
				Function: types.ChatToolFunction{
					Name:        mcp.ToolPodsGet,
					Description: "Get a Kubernetes Pod via containers/kubernetes-mcp-server",
				},
			},
			{
				Type: "function",
				Function: types.ChatToolFunction{
					Name:        mcp.ToolResourcesGet,
					Description: "Get a Kubernetes resource via containers/kubernetes-mcp-server",
				},
			},
			{
				Type: "function",
				Function: types.ChatToolFunction{
					Name:        "copilot_web_search",
					Description: "Search web via Copilot extension",
				},
			},
		},
	}
	body, _ := json.Marshal(chatReq)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var chatResp types.ChatCompletionResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &chatResp)

	if chatResp.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("expected finish_reason='tool_calls', got %s", chatResp.Choices[0].FinishReason)
	}
	if len(chatResp.Choices[0].Message.ToolCalls) != 3 {
		t.Fatalf("expected 3 tool_calls (pods_get, resources_get, copilot_web_search), got %d", len(chatResp.Choices[0].Message.ToolCalls))
	}
}
