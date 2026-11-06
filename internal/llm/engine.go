package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/mcp"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
)

const DefaultModelName = "kube-doctor-1.35-ocp-4.22"

// SystemPrompt defines the specialized identity, version scope (K8s >= 1.35, OCP >= 4.22),
// remediation policies, and github.com/containers/kubernetes-mcp-server tool-calling behavior.
const SystemPrompt = `You are kube-doctor-1.35-ocp-4.22, a specialized Autonomous Site Reliability & Cluster Remediation LLM purpose-built for Kubernetes >= 1.35 and Red Hat OpenShift >= 4.22.

Core Capabilities & Governance Rules:
1. Version Expertise:
   - Kubernetes 1.35+: InPlacePodVerticalScaling (GA via pods/resize subresource), Dynamic Resource Allocation (resource.k8s.io/v1beta1), native SidecarContainers, UserNamespaces, cgroup v2 PSI memory pressure metrics, StatefulSet auto-PVC retention.
   - OpenShift 4.22+: Machine API (machine.openshift.io/v1beta1) & Cluster API (cluster.x-k8s.io/v1beta1), OVN-Kubernetes live migration, Hosted Control Planes, ClusterOperator triage.
2. Native Integration with github.com/containers/kubernetes-mcp-server:
   - You natively use and emit tool_calls for containers/kubernetes-mcp-server tools:
     * events_list (namespace, fieldSelector)
     * pods_get (name, namespace), pods_log (name, namespace, container, previous, tail), pods_top (name, namespace)
     * nodes_stats_summary (name) for kubelet Summary API & cgroup v2 PSI metrics
     * resources_get (apiVersion, kind, name, namespace), resources_list (apiVersion, kind, namespace)
     * resources_create_or_update (resource) via Server-Side Apply
     * resources_scale (apiVersion, kind, name, namespace, scale)
3. Criticality & Severity Driven Action:
   - Pod OOMKilled (ExitCode 137) in StatefulSet (within Namespace ResourceQuota): Classify as P1-HIGH / SEV-2 (AUTO_REMEDIATE). Immediately update StatefulSet container memory request/limit (via resources_create_or_update) and invoke K8s 1.35+ pods/resize.
   - Namespace ResourceQuota Exceeded: Classify as P0-CRITICAL / SEV-1 (APPROVAL_REQUIRED). Create a kubedoctor.io/v1alpha1 RemediationRequest CRD and open a GitOps Pull Request to expand the Namespace ResourceQuota first, chaining the StatefulSet memory update upon approval.
   - OpenShift Machine Missing / Node Capacity Deficit: Classify as P0-CRITICAL / SEV-1 (APPROVAL_REQUIRED). Create a RemediationRequest CRD and GitOps Pull Request to autoscale the OpenShift MachineSet replicas (via resources_scale) upon approval.
4. Copilot / Agent MCP & Web Collaboration:
   - When you need live cluster resources from the caller's containers/kubernetes-mcp-server or external CVE/errata web search from the calling Copilot Extension, emit tool_calls (e.g., pods_get, pods_log, resources_get, nodes_stats_summary, or web_search) and synthesize the role="tool" output on the next turn.`

// Engine coordinates RAG retrieval, bidirectional Copilot/Agent tool-calling
// (including github.com/containers/kubernetes-mcp-server tools & Web search),
// upstream fine-tuned vLLM/Ollama inference, and cluster remediation execution.
type Engine struct {
	ragEngine         *rag.Engine
	remediationEngine *remediation.Engine
	k8sClient         *k8s.Client
	k8sMCPClient      *mcp.K8sMCPClient
	upstreamBaseURL   string
	upstreamAPIKey    string
	httpClient        *http.Client
}

// NewEngine creates a new specialized LLM Engine wired to RAG, Remediation, live K8s API, and containers/kubernetes-mcp-server.
// Defaults UPSTREAM_LLM_BASE_URL to http://127.0.0.1:11434 (Ollama) if not explicitly set.
func NewEngine(ragEngine *rag.Engine, remEngine *remediation.Engine, k8sClient ...*k8s.Client) *Engine {
	var kc *k8s.Client
	if len(k8sClient) > 0 {
		kc = k8sClient[0]
	}
	upstreamURL := strings.TrimRight(os.Getenv("UPSTREAM_LLM_BASE_URL"), "/")
	if upstreamURL == "" && os.Getenv("DISABLE_UPSTREAM_LLM") != "true" {
		upstreamURL = "http://127.0.0.1:11434"
	}
	return &Engine{
		ragEngine:         ragEngine,
		remediationEngine: remEngine,
		k8sClient:         kc,
		k8sMCPClient:      mcp.NewK8sMCPClient(),
		upstreamBaseURL:   upstreamURL,
		upstreamAPIKey:    os.Getenv("UPSTREAM_LLM_API_KEY"),
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// DiagnoseAndRemediateEvent uses the kube-doctor LLM + RAG + containers/kubernetes-mcp-server
// to inspect live cluster telemetry for a discovered event (from the autonomous watcher or
// Prometheus Alertmanager webhook), reason over the root cause, and execute or stage remediation.
func (e *Engine) DiagnoseAndRemediateEvent(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, string, error) {
	if e.k8sClient != nil {
		e.k8sClient.EnrichDiagnosticEvent(ctx, &ev)
	}

	// 1. Pull live telemetry via containers/kubernetes-mcp-server tools (pods_get, pods_log, resources_get)
	var mcpTelemetry []string
	if e.k8sMCPClient != nil {
		if ev.PodName != "" {
			if podOut, err := e.k8sMCPClient.GetPod(ctx, ev.Namespace, ev.PodName); err == nil && podOut != "" {
				mcpTelemetry = append(mcpTelemetry, fmt.Sprintf("pods_get(%s/%s): %s", ev.Namespace, ev.PodName, truncateStr(podOut, 600)))
			}
			if logOut, err := e.k8sMCPClient.GetPodLogs(ctx, ev.Namespace, ev.PodName, ev.ContainerName, true, 30); err == nil && logOut != "" {
				mcpTelemetry = append(mcpTelemetry, fmt.Sprintf("pods_log(%s/%s): %s", ev.Namespace, ev.PodName, truncateStr(logOut, 400)))
			}
		}
		if ev.OwnerName != "" && ev.OwnerKind != "" {
			if wlOut, err := e.k8sMCPClient.GetResource(ctx, "apps/v1", ev.OwnerKind, ev.Namespace, ev.OwnerName); err == nil && wlOut != "" {
				mcpTelemetry = append(mcpTelemetry, fmt.Sprintf("resources_get(%s/%s): %s", ev.OwnerKind, ev.OwnerName, truncateStr(wlOut, 400)))
			}
		}
	}

	// 2. Retrieve specialized K8s >= 1.35 / OCP >= 4.22 RAG runbooks
	query := fmt.Sprintf("%s %s %s %s", ev.Reason, ev.Message, ev.OwnerKind, ev.QuotaName)
	ragDocs := e.ragEngine.Retrieve(query, 3)

	// 3. Execute or stage remediation via the governance-aware engine
	assessment, err := e.remediationEngine.EvaluateAndAct(ctx, ev)
	if err != nil {
		return nil, "", err
	}

	// 4. Ask the LLM (upstream vLLM/Ollama fine-tuned model or built-in specialist synthesizer)
	// to produce the final RootCauseAnalysis incorporating live MCP telemetry and RAG knowledge.
	telemetrySummary := strings.Join(mcpTelemetry, "\n")
	prompt := fmt.Sprintf(
		"Autonomous Cluster Trigger: namespace=%s pod=%s owner=%s/%s reason=%s exitCode=%d currentMemoryLimit=%s quota=%s (%s/%s). Message: %s",
		ev.Namespace, ev.PodName, ev.OwnerKind, ev.OwnerName, ev.Reason, ev.ExitCode,
		ev.CurrentMemoryLim, ev.QuotaName, ev.QuotaMemoryUsed, ev.QuotaMemoryLimit, ev.Message,
	)

	var llmReasoning string
	if e.upstreamBaseURL != "" {
		chatReq := types.ChatCompletionRequest{
			Model: DefaultModelName,
			Messages: []types.ChatMessage{
				{Role: "user", Content: prompt + "\n\nLive MCP Telemetry:\n" + telemetrySummary},
			},
		}
		if upstreamResp, upErr := e.callUpstreamModel(ctx, chatReq, ragDocs, assessment); upErr == nil && len(upstreamResp.Choices) > 0 {
			llmReasoning = upstreamResp.Choices[0].Message.Content
			assessment.RootCauseAnalysis = llmReasoning
		}
	}
	if llmReasoning == "" {
		llmReasoning = e.synthesizeSpecialistResponse(prompt, telemetrySummary, ragDocs, assessment)
	}

	return assessment, llmReasoning, nil
}

func truncateStr(s string, maxLen int) string {
	s = strings.TrimSpace(s)
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ChatCompletion processes an OpenAI-compatible ChatCompletionRequest from any Copilot Extension,
// IDE agent, or cluster watcher.
func (e *Engine) ChatCompletion(ctx context.Context, req types.ChatCompletionRequest) (*types.ChatCompletionResponse, error) {
	if req.Model == "" {
		req.Model = DefaultModelName
	}

	lastUserMsg, hasToolResult, toolResultContent := inspectMessages(req.Messages)

	// 1. Check if the LLM should ask the calling Copilot Extension / Agent for
	// github.com/containers/kubernetes-mcp-server tool execution or Web information first.
	if !hasToolResult {
		if toolCalls := e.shouldRequestExternalMCPOrWebInfo(lastUserMsg, req.Tools); len(toolCalls) > 0 {
			return &types.ChatCompletionResponse{
				ID:      fmt.Sprintf("chatcmpl-kd-%d", time.Now().UnixNano()),
				Object:  "chat.completion",
				Created: time.Now().Unix(),
				Model:   req.Model,
				Choices: []types.ChatCompletionChoice{
					{
						Index: 0,
						Message: types.ChatMessage{
							Role:      "assistant",
							Content:   "I need live cluster telemetry from `containers/kubernetes-mcp-server` and/or web advisory search before finalizing the Kubernetes 1.35+ / OpenShift 4.22+ remediation plan.",
							ToolCalls: toolCalls,
						},
						FinishReason: "tool_calls",
					},
				},
				Usage: types.ChatCompletionUsage{
					PromptTokens:     196,
					CompletionTokens: 72,
					TotalTokens:      268,
				},
			}, nil
		}
	}

	// 2. Retrieve relevant Kubernetes 1.35+ / OpenShift 4.22+ RAG documents
	ragDocs := e.ragEngine.Retrieve(lastUserMsg+" "+toolResultContent, 3)

	// 3. Extract or discover a real DiagnosticEvent from the live cluster first, or fall back to prompt inference
	var assessment *types.TriageAssessment
	if req.DiagnosticEvent != nil {
		assessment, _ = e.remediationEngine.EvaluateAndAct(ctx, *req.DiagnosticEvent)
	} else if e.k8sClient != nil && !e.k8sClient.IsSimulated() {
		if liveEvents, err := e.k8sClient.ScanClusterHealth(ctx); err == nil && len(liveEvents) > 0 {
			assessment, _ = e.remediationEngine.EvaluateAndAct(ctx, liveEvents[0])
		} else if inferredEv, ok := inferDiagnosticEventFromPrompt(lastUserMsg + " " + toolResultContent); ok {
			assessment, _ = e.remediationEngine.EvaluateAndAct(ctx, inferredEv)
		}
	} else if inferredEv, ok := inferDiagnosticEventFromPrompt(lastUserMsg + " " + toolResultContent); ok {
		assessment, _ = e.remediationEngine.EvaluateAndAct(ctx, inferredEv)
	}

	// 4. If an upstream fine-tuned vLLM/Ollama backend is configured, call it with RAG + Triage context
	if e.upstreamBaseURL != "" {
		if resp, err := e.callUpstreamModel(ctx, req, ragDocs, assessment); err == nil {
			resp.TriageAssessment = assessment
			return resp, nil
		}
	}

	// 5. Synthesize authoritative Kubernetes 1.35+ / OpenShift 4.22+ specialist response
	replyText := e.synthesizeSpecialistResponse(lastUserMsg, toolResultContent, ragDocs, assessment)

	return &types.ChatCompletionResponse{
		ID:      fmt.Sprintf("chatcmpl-kd-%d", time.Now().UnixNano()),
		Object:  "chat.completion",
		Created: time.Now().Unix(),
		Model:   req.Model,
		Choices: []types.ChatCompletionChoice{
			{
				Index: 0,
				Message: types.ChatMessage{
					Role:    "assistant",
					Content: replyText,
				},
				FinishReason: "stop",
			},
		},
		Usage: types.ChatCompletionUsage{
			PromptTokens:     len(lastUserMsg)/4 + 250,
			CompletionTokens: len(replyText) / 4,
			TotalTokens:      (len(lastUserMsg)+len(replyText))/4 + 250,
		},
		TriageAssessment: assessment,
	}, nil
}

// shouldRequestExternalMCPOrWebInfo checks if the prompt requires tools from
// github.com/containers/kubernetes-mcp-server (e.g. pods_get, pods_log, resources_get,
// nodes_stats_summary, events_list) or Web search from the calling Copilot agent.
func (e *Engine) shouldRequestExternalMCPOrWebInfo(prompt string, callerTools []types.ChatTool) []types.ToolCall {
	lower := strings.ToLower(prompt)
	needsWeb := strings.Contains(lower, "cve-") ||
		strings.Contains(lower, "search web") ||
		strings.Contains(lower, "latest errata") ||
		strings.Contains(lower, "red hat advisory") ||
		strings.Contains(lower, "release notes") ||
		strings.Contains(lower, "ask web")

	// Check if caller passed any github.com/containers/kubernetes-mcp-server tools
	hasK8sMCPTools := false
	callerToolMap := make(map[string]bool)
	for _, t := range callerTools {
		callerToolMap[t.Function.Name] = true
		switch t.Function.Name {
		case mcp.ToolPodsGet, mcp.ToolPodsLog, mcp.ToolPodsTop, mcp.ToolEventsList,
			mcp.ToolNodesStatsSummary, mcp.ToolResourcesGet, mcp.ToolResourcesList,
			mcp.ToolResourcesCreateOrUpdate, mcp.ToolResourcesScale:
			hasK8sMCPTools = true
		}
	}

	needsMCP := hasK8sMCPTools ||
		strings.Contains(lower, "fetch mcp") ||
		strings.Contains(lower, "from mcp") ||
		strings.Contains(lower, "kubernetes-mcp-server") ||
		strings.Contains(lower, "prometheus mcp") ||
		strings.Contains(lower, "ask mcp") ||
		strings.Contains(lower, "need mcp")

	if !needsWeb && !needsMCP {
		return nil
	}

	var calls []types.ToolCall

	if needsMCP {
		// Prefer github.com/containers/kubernetes-mcp-server native tool names when available
		switch {
		case callerToolMap[mcp.ToolPodsGet]:
			args, _ := json.Marshal(map[string]any{
				"namespace": "production",
				"name":      "postgres-ha-0",
			})
			calls = append(calls, types.ToolCall{
				ID:   fmt.Sprintf("call_k8smcp_pod_%d", time.Now().UnixNano()),
				Type: "function",
				Function: types.ToolCallFunction{
					Name:      mcp.ToolPodsGet,
					Arguments: string(args),
				},
			})
			if callerToolMap[mcp.ToolResourcesGet] {
				resArgs, _ := json.Marshal(map[string]any{
					"apiVersion": "v1",
					"kind":       "ResourceQuota",
					"namespace":  "production",
					"name":       "production-compute-quota",
				})
				calls = append(calls, types.ToolCall{
					ID:   fmt.Sprintf("call_k8smcp_quota_%d", time.Now().UnixNano()),
					Type: "function",
					Function: types.ToolCallFunction{
						Name:      mcp.ToolResourcesGet,
						Arguments: string(resArgs),
					},
				})
			}

		case callerToolMap[mcp.ToolPodsLog]:
			args, _ := json.Marshal(map[string]any{
				"namespace": "production",
				"name":      "postgres-ha-0",
				"previous":  true,
				"tail":      100,
			})
			calls = append(calls, types.ToolCall{
				ID:   fmt.Sprintf("call_k8smcp_log_%d", time.Now().UnixNano()),
				Type: "function",
				Function: types.ToolCallFunction{
					Name:      mcp.ToolPodsLog,
					Arguments: string(args),
				},
			})

		default:
			toolName := mcp.ToolResourcesGet
			for _, t := range callerTools {
				if strings.Contains(strings.ToLower(t.Function.Name), "mcp") ||
					strings.Contains(strings.ToLower(t.Function.Name), "pods_") ||
					strings.Contains(strings.ToLower(t.Function.Name), "resources_") ||
					strings.Contains(strings.ToLower(t.Function.Name), "prometheus") {
					toolName = t.Function.Name
					break
				}
			}
			args, _ := json.Marshal(map[string]any{
				"apiVersion":     "apps/v1",
				"kind":           "StatefulSet",
				"namespace":      "production",
				"name":           "postgres-ha",
				"query":          prompt,
				"clusterVersion": "1.35",
				"ocpVersion":     "4.22",
			})
			calls = append(calls, types.ToolCall{
				ID:   fmt.Sprintf("call_mcp_%d", time.Now().UnixNano()),
				Type: "function",
				Function: types.ToolCallFunction{
					Name:      toolName,
					Arguments: string(args),
				},
			})
		}
	}

	if needsWeb {
		toolName := "web_search"
		for _, t := range callerTools {
			if strings.Contains(strings.ToLower(t.Function.Name), "web") ||
				strings.Contains(strings.ToLower(t.Function.Name), "search") ||
				strings.Contains(strings.ToLower(t.Function.Name), "browser") {
				toolName = t.Function.Name
				break
			}
		}
		args, _ := json.Marshal(map[string]any{
			"query": "Kubernetes 1.35 OpenShift 4.22 " + prompt,
		})
		calls = append(calls, types.ToolCall{
			ID:   fmt.Sprintf("call_web_%d", time.Now().UnixNano()),
			Type: "function",
			Function: types.ToolCallFunction{
				Name:      toolName,
				Arguments: string(args),
			},
		})
	}

	return calls
}

func inspectMessages(msgs []types.ChatMessage) (lastUser string, hasToolResult bool, toolResultContent string) {
	var toolOutputs []string
	for _, m := range msgs {
		switch m.Role {
		case "user":
			lastUser = m.Content
		case "tool":
			hasToolResult = true
			toolOutputs = append(toolOutputs, fmt.Sprintf("[Tool %s Output]: %s", m.ToolCallID, m.Content))
		}
	}
	return lastUser, hasToolResult, strings.Join(toolOutputs, "\n")
}

func inferDiagnosticEventFromPrompt(text string) (types.DiagnosticEvent, bool) {
	lower := strings.ToLower(text)
	ev := types.DiagnosticEvent{
		ClusterVersion:   "v1.35.2",
		OpenShiftVersion: "4.22.1",
		Namespace:        "production",
		Message:          text,
		Timestamp:        time.Now().UTC(),
	}

	if strings.Contains(lower, "cnpg") || strings.Contains(lower, "pg-cluster") || strings.Contains(lower, "postgresql.cnpg.io") {
		ev.OwnerAPIVersion = "postgresql.cnpg.io/v1"
		ev.OwnerKind = "Cluster"
		ev.OwnerName = "pg-cluster"
		ev.OperatorAdapter = "cloudnative-pg"
		ev.PodName = "pg-cluster-1"
		ev.ContainerName = "postgres"
		ev.CurrentMemoryReq = "128Mi"
		ev.CurrentMemoryLim = "256Mi"
		if strings.Contains(lower, "disk") || strings.Contains(lower, "storage") || strings.Contains(lower, "wal") {
			ev.Reason = "PVCFilesystemFull"
			ev.CurrentStorage = "10Gi"
		} else {
			ev.ExitCode = 137
			ev.Reason = "OOMKilled"
		}
		if strings.Contains(lower, "quota") || strings.Contains(lower, "exceed") {
			ev.QuotaName = "production-compute-quota"
			ev.QuotaMemoryLimit = "512Mi"
			ev.QuotaMemoryUsed = "384Mi"
		}
		return ev, true
	}

	if strings.Contains(lower, "noobaa") || strings.Contains(lower, "backingstore") || strings.Contains(lower, "bucketclass") || strings.Contains(lower, "storagecluster") {
		ev.Namespace = "openshift-storage"
		ev.OwnerAPIVersion = "noobaa.io/v1alpha1"
		ev.OwnerKind = "NooBaa"
		ev.OwnerName = "noobaa"
		ev.OperatorAdapter = "noobaa-odf"
		ev.PodName = "noobaa-core-0"
		ev.Reason = "OperatorCRDegraded"
		return ev, true
	}

	if strings.Contains(lower, "machineset") || strings.Contains(lower, "machine missing") || strings.Contains(lower, "machinefailed") {
		ev.Reason = "MachineMissing"
		ev.MachineSetName = "ocp422-worker-us-east-1a"
		ev.ExpectedMachines = 3
		ev.ReadyMachines = 2
		return ev, true
	}

	if strings.Contains(lower, "oom") || strings.Contains(lower, "137") || strings.Contains(lower, "statefulset") || strings.Contains(lower, "resourcequota") {
		ev.ExitCode = 137
		ev.Reason = "OOMKilled"
		ev.OwnerKind = "StatefulSet"
		ev.OwnerName = "postgres-ha"
		ev.PodName = "postgres-ha-0"
		ev.ContainerName = "postgres"
		ev.CurrentMemoryReq = "512Mi"
		ev.CurrentMemoryLim = "1Gi"

		if strings.Contains(lower, "quota") || strings.Contains(lower, "exceed") {
			ev.QuotaName = "production-compute-quota"
			ev.QuotaMemoryLimit = "2Gi"
			ev.QuotaMemoryUsed = "1800Mi"
		}
		return ev, true
	}

	keywords := []string{
		"limitrange", "imagepullbackoff", "errimagepull", "sidecar",
		"liveness", "readiness", "evicted", "diskpressure", "ephemeral-storage",
		"terminating", "finalizer", "hpa", "maxreplicas", "pdb", "poddisruptionbudget",
		"dra", "resourceclaim", "deviceclass", "pvc", "storageclass", "filesystem",
		"multi-attach", "volumeattachment", "endpoint", "networkpolicy", "ovn",
		"route", "503", "ingress", "coredns", "dns", "nodenotready", "pleg",
		"clusteroperator", "degraded", "machineconfig", "etcd", "quorum",
		"certificate", "csr", "forbidden", "rbac", "scc", "crashloopbackoff",
	}
	for _, kw := range keywords {
		if strings.Contains(lower, kw) {
			ev.Reason = kw
			return ev, true
		}
	}

	return ev, false
}

func (e *Engine) synthesizeSpecialistResponse(userPrompt, toolResult string, ragDocs []rag.Document, assessment *types.TriageAssessment) string {
	var b strings.Builder

	b.WriteString("### `kube-doctor` (Kubernetes 1.35+ / OpenShift 4.22+ Specialist Assessment)\n\n")

	if toolResult != "" {
		b.WriteString("#### `containers/kubernetes-mcp-server` & Web Telemetry Integrated\n")
		b.WriteString(fmt.Sprintf("> Synthesized external agent context: `%s`\n\n", strings.TrimSpace(toolResult)))
	}

	if assessment != nil {
		b.WriteString(fmt.Sprintf("- **Issue Type**: `%s`\n", assessment.IssueType))
		b.WriteString(fmt.Sprintf("- **Criticality**: **`%s`** | **Severity**: **`%s`**\n", assessment.Criticality, assessment.Severity))
		if assessment.OperatorAdapter != "" {
			b.WriteString(fmt.Sprintf("- **Operator Adapter**: `%s`\n", assessment.OperatorAdapter))
		}
		if len(assessment.OwnershipChain) > 0 {
			var hops []string
			for _, link := range assessment.OwnershipChain {
				hops = append(hops, fmt.Sprintf("`%s/%s` (%s)", link.Kind, link.Name, link.APIVersion))
			}
			b.WriteString(fmt.Sprintf("- **Resolved `ownerReferences` Lineage**: %s\n", strings.Join(hops, " -> ")))
		}
		b.WriteString(fmt.Sprintf("- **Executive Summary**: %s\n\n", assessment.Summary))

		b.WriteString("#### Root Cause Analysis\n")
		b.WriteString(assessment.RootCauseAnalysis + "\n\n")

		if len(assessment.PlannedActions) > 0 {
			b.WriteString("#### Remediation Actions Taken / Staged\n")
			for _, act := range assessment.PlannedActions {
				b.WriteString(fmt.Sprintf("%d. **`[%s]` `%s` on `%s/%s` (Namespace: `%s`)**\n",
					act.StepOrder, act.Status, act.Operation, act.TargetKind, act.TargetName, act.TargetNamespace))
				b.WriteString(fmt.Sprintf("   - **Policy**: `%s`\n", act.Policy))
				b.WriteString(fmt.Sprintf("   - **Action**: %s\n", act.Description))
				if act.ApprovalID != "" {
					b.WriteString(fmt.Sprintf("   - **Approval ID**: `%s` (Approve via `POST /v1/approvals/%s/approve` or merge GitOps PR `%s`)\n",
						act.ApprovalID, act.ApprovalID, act.GitOpsPRURL))
				}
				b.WriteString(fmt.Sprintf("   - **Manifest / Patch Payload (`resources_create_or_update` / `resources_scale`)**:\n```json\n%s\n```\n", act.PatchPayload))
			}
			b.WriteString("\n")
		}
	}

	if len(ragDocs) > 0 {
		b.WriteString("#### Reference Runbooks (K8s 1.35+ / OpenShift 4.22+ RAG)\n")
		for _, d := range ragDocs {
			b.WriteString(fmt.Sprintf("- **%s** (*%s*): %s\n", d.Title, d.Version, d.Content))
		}
	}

	return b.String()
}

func (e *Engine) callUpstreamModel(ctx context.Context, req types.ChatCompletionRequest, ragDocs []rag.Document, assessment *types.TriageAssessment) (*types.ChatCompletionResponse, error) {
	var ragContext strings.Builder
	ragContext.WriteString(SystemPrompt + "\n\nRetrieved Kubernetes 1.35+ / OpenShift 4.22+ Knowledge:\n")
	for _, d := range ragDocs {
		ragContext.WriteString(fmt.Sprintf("- [%s]: %s\n", d.Title, d.Content))
	}
	if assessment != nil {
		assessmentBytes, _ := json.MarshalIndent(assessment, "", "  ")
		ragContext.WriteString(fmt.Sprintf("\nExecuted/Staged Cluster Remediation Triage:\n%s\n", string(assessmentBytes)))
	}

	augmentedMsgs := []types.ChatMessage{
		{Role: "system", Content: ragContext.String()},
	}
	augmentedMsgs = append(augmentedMsgs, req.Messages...)

	upReq := req
	upReq.Messages = augmentedMsgs
	bodyBytes, err := json.Marshal(upReq)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.upstreamBaseURL+"/v1/chat/completions", bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if e.upstreamAPIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+e.upstreamAPIKey)
	}

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("upstream LLM returned status %d", resp.StatusCode)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var out types.ChatCompletionResponse
	if err := json.Unmarshal(respBytes, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
