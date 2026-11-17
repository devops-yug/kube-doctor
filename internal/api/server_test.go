package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/luigi/kube-doctor/internal/api"
	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/hub"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/llm"
	"github.com/luigi/kube-doctor/internal/mcp"
	"github.com/luigi/kube-doctor/internal/operator"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
	"github.com/luigi/kube-doctor/internal/watcher"
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

func TestAllHTTPGatewayEndpointsAndErrorPaths(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	t.Setenv("DISABLE_UPSTREAM_LLM", "true")

	ragEngine := rag.NewEngine("../../knowledge/k8s_1_35_ocp_4_22.json")
	k8sClient := k8s.NewClient()
	k8sMCPClient := mcp.NewK8sMCPClient()
	gitopsMgr := gitops.NewManager(k8sClient)
	remEngine := remediation.NewEngine(k8sClient, gitopsMgr, k8sMCPClient)
	llmEngine := llm.NewEngine(ragEngine, remEngine, k8sClient)
	mcpServer := mcp.NewServer(ragEngine, remEngine, gitopsMgr, k8sMCPClient)
	srv := api.NewServer(llmEngine, remEngine, gitopsMgr, k8sClient, mcpServer)

	doReq := func(method, path, body string) *httptest.ResponseRecorder {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
		} else {
			r = httptest.NewRequest(method, path, nil)
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, r)
		return rec
	}

	// 1. Test OPTIONS CORS preflight, /healthz, /ui, /v1/models, /v1/audit
	if rec := doReq(http.MethodOptions, "/v1/models", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("/healthz failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/ui", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "OpenShift 4.22+") {
		t.Fatalf("/ui failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/models", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/models failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/audit", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/audit failed: %d", rec.Code)
	}

	// 2. Test endpoints before watcher/hub/operator are attached (nil-safety paths)
	if rec := doReq(http.MethodPost, "/v1/cluster/scan", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /v1/cluster/scan without watcher, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/incidents", ""); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /v1/incidents without watcher, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/effectiveness", ""); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /v1/effectiveness without watcher, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/webhooks/alertmanager", `{}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /v1/webhooks/alertmanager without watcher, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hub/clusters", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /v1/hub/clusters without hubManager, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hub/clusters/c1", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /v1/hub/clusters/c1 without hubManager, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/operator/status", ""); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /v1/operator/status without operatorController, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/operator/reconcile", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 on /v1/operator/reconcile without operatorController, got %d", rec.Code)
	}

	// 3. Attach Watcher and OperatorController (first without HubManager to test watcher-only /v1/incidents)
	w := watcher.NewWatcher(k8sClient, remEngine, llmEngine)
	opCtrl := operator.NewController(k8sClient, gitopsMgr, remEngine)
	srv.SetWatcher(w)
	srv.SetOperatorController(opCtrl)

	if rec := doReq(http.MethodGet, "/healthz", ""); rec.Code != http.StatusOK {
		t.Fatalf("/healthz with watcher failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/cluster/scan?force=true", `{"force":true}`); rec.Code != http.StatusOK {
		t.Fatalf("/v1/cluster/scan failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodDelete, "/v1/cluster/scan", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on DELETE /v1/cluster/scan, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/incidents", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/incidents (active only, no hub) failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/incidents?includeResolved=true", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/incidents?includeResolved=true (no hub) failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/incidents", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /v1/incidents, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/effectiveness", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/effectiveness failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/operator/status", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/operator/status failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/operator/reconcile", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/operator/reconcile failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodDelete, "/v1/operator/reconcile", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on DELETE /v1/operator/reconcile, got %d", rec.Code)
	}

	// 4. Attach HubManager and test /v1/incidents + /v1/hub/clusters routes
	hm := hub.NewManager("hub", k8sClient, remEngine, w)
	srv.SetHubManager(hm)

	if rec := doReq(http.MethodGet, "/v1/incidents", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/incidents (with hub) failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/incidents?includeResolved=true", ""); rec.Code != http.StatusOK {
		t.Fatalf("/v1/incidents?includeResolved=true (with hub) failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hub/clusters", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/hub/clusters failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/hub/clusters", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /v1/hub/clusters POST, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/hub/clusters", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty clusterId /v1/hub/clusters POST, got %d", rec.Code)
	}
	hbJSON := `{"clusterId":"spoke-1","displayName":"Spoke 1","kubernetesVersion":"v1.35.2"}`
	if rec := doReq(http.MethodPost, "/v1/hub/clusters/register", hbJSON); rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/hub/clusters/register failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hub/clusters/spoke-1", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/hub/clusters/spoke-1 failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/hub/clusters/spoke-1/commands", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid command JSON, got %d", rec.Code)
	}
	cmdRec := doReq(http.MethodPost, "/v1/hub/clusters/spoke-1/commands", `{"actionId":"a1","targetKind":"Pod","targetName":"p1"}`)
	if cmdRec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/hub/clusters/spoke-1/commands failed: %d", cmdRec.Code)
	}
	var dispatched hub.AgentCommand
	_ = json.Unmarshal(cmdRec.Body.Bytes(), &dispatched)
	if rec := doReq(http.MethodGet, "/v1/hub/clusters/spoke-1/commands", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/hub/clusters/spoke-1/commands failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/hub/clusters/spoke-1/commands/"+dispatched.CommandID+"/ack", `{"status":"EXECUTED","resultMessage":"ok"}`); rec.Code != http.StatusOK {
		t.Fatalf("POST command ack failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/hub/clusters/spoke-1/commands/missing/ack", `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on missing command ack, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hub/clusters/spoke-1/unknown", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on unknown hub subroute, got %d", rec.Code)
	}

	// 5. Chat completions (streaming + error paths) & Copilot Extension
	if rec := doReq(http.MethodGet, "/v1/chat/completions", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /v1/chat/completions, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/chat/completions", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /v1/chat/completions, got %d", rec.Code)
	}
	streamChat := `{"model":"kube-doctor-1.35-ocp-4.22","stream":true,"messages":[{"role":"user","content":"Why is pod oom-0 OOMKilled?"}]}`
	if rec := doReq(http.MethodPost, "/v1/chat/completions", streamChat); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "data: [DONE]") {
		t.Fatalf("streaming /v1/chat/completions failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/copilot/agent", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /copilot/agent, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/copilot/agent", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /copilot/agent, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/copilot/agent", `{"messages":[{"role":"user","content":"Diagnose cluster"}]}`); rec.Code != http.StatusOK {
		t.Fatalf("/copilot/agent failed: %d", rec.Code)
	}

	// 6. Diagnostics Ingest, Alertmanager Webhook, Hierarchy, Adapters
	if rec := doReq(http.MethodGet, "/v1/diagnostics/ingest", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /v1/diagnostics/ingest, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/diagnostics/ingest", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /v1/diagnostics/ingest, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/webhooks/alertmanager", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /v1/webhooks/alertmanager, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/webhooks/alertmanager", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /v1/webhooks/alertmanager, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/webhooks/alertmanager", `{"status":"firing","alerts":[{"status":"firing","labels":{"alertname":"KubePodOOMKilled","namespace":"default","pod":"p-0"}}]}`); rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/webhooks/alertmanager failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hierarchy", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on /v1/hierarchy without name, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/hierarchy?name=pg-cluster-1", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/hierarchy failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/adapters", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/adapters failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/adapters", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid POST /v1/adapters, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/adapters", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty POST /v1/adapters, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/adapters", `{"name":"custom-op","primaryGvk":{"apiVersion":"custom.io/v1","kind":"Custom"}}`); rec.Code != http.StatusCreated {
		t.Fatalf("POST /v1/adapters failed: %d", rec.Code)
	}

	// 7. Approvals & GitOps OAuth / SCM Webhooks
	if rec := doReq(http.MethodPost, "/v1/approvals", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /v1/approvals, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/approvals", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/approvals failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/approvals/123/approve", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /v1/approvals/123/approve, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/approvals/only-one-part", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on /v1/approvals/only-one-part, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/approvals/missing/approve", `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on missing approve, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/approvals/missing/reject", `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on missing reject, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/approvals/missing/create-pr", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on missing create-pr, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/approvals/missing/unknown", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on unknown approval action, got %d", rec.Code)
	}

	// Create a real pending approval to test create-pr and reject
	appReq, _ := gitopsMgr.CreateApprovalRequest(httptest.NewRequest(http.MethodGet, "/", nil).Context(), types.IssueNodeNotReady, types.CriticalityP0Critical, types.SeveritySev1, "test approval", types.RemediationAction{
		ActionID: "act-99", TargetKind: "Node", TargetName: "worker-1",
	}, nil)
	if rec := doReq(http.MethodPost, "/v1/approvals/"+appReq.ID+"/create-pr", `{"provider":"gitlab"}`); rec.Code != http.StatusOK {
		t.Fatalf("POST create-pr failed: %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(http.MethodPost, "/v1/approvals/"+appReq.ID+"/reject", `{"approvedBy":"admin"}`); rec.Code != http.StatusOK {
		t.Fatalf("POST reject failed: %d", rec.Code)
	}

	// GitOps Status, Config, OAuth, and Webhooks
	if rec := doReq(http.MethodPost, "/v1/gitops/status", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on POST /v1/gitops/status, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/gitops/status", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/gitops/status failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/gitops/config", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /v1/gitops/config, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/gitops/config", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /v1/gitops/config, got %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/gitops/config", `{"provider":"bitbucket"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on unsupported provider /v1/gitops/config, got %d", rec.Code)
	}
	cfgBody := `{"approvalMode":"HYBRID","inClusterAllowed":true,"provider":"github","clientId":"cid","repository":"org/repo","github":{"clientId":"gh"},"gitlab":{"clientId":"gl"}}`
	if rec := doReq(http.MethodPost, "/v1/gitops/config", cfgBody); rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/gitops/config failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/gitops/oauth/short", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on short oauth path, got %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/gitops/oauth/github/authorize", ""); rec.Code != http.StatusOK {
		t.Fatalf("GET /v1/gitops/oauth/github/authorize failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodGet, "/v1/gitops/oauth/github/authorize?redirect=true", ""); rec.Code != http.StatusFound {
		t.Fatalf("GET /v1/gitops/oauth/github/authorize?redirect=true failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/gitops/oauth/github/callback", `{"code":"sim-code","state":"s1"}`); rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/gitops/oauth/github/callback failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/gitops/oauth/github/disconnect", ""); rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/gitops/oauth/github/disconnect failed: %d", rec.Code)
	}
	if rec := doReq(http.MethodPost, "/v1/gitops/oauth/github/unknown", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on unknown oauth action, got %d", rec.Code)
	}

	// GitHub and GitLab webhooks
	if rec := doReq(http.MethodGet, "/v1/webhooks/github", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /v1/webhooks/github, got %d", rec.Code)
	}
	appGH, _ := gitopsMgr.CreateApprovalRequest(httptest.NewRequest(http.MethodGet, "/", nil).Context(), types.IssueNodeNotReady, types.CriticalityP0Critical, types.SeveritySev1, "gh webhook test", types.RemediationAction{
		ActionID: "act-gh", TargetKind: "Node", TargetName: "worker-2",
	}, nil)
	ghPayload := `{"action":"closed","pull_request":{"merged":true,"title":"[` + appGH.ID + `] fix","head":{"ref":"kube-doctor/` + appGH.CRDName + `"},"merged_by":{"login":"octocat"}}}`
	if rec := doReq(http.MethodPost, "/v1/webhooks/github", ghPayload); rec.Code != http.StatusOK {
		t.Fatalf("POST /v1/webhooks/github failed: %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(http.MethodPost, "/v1/webhooks/gitlab", `invalid`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid /v1/webhooks/gitlab, got %d", rec.Code)
	}
}

