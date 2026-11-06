package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/luigi/kube-doctor/internal/adapters"
	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/hub"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/llm"
	"github.com/luigi/kube-doctor/internal/mcp"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
	"github.com/luigi/kube-doctor/internal/watcher"
)

// Server exposes the OpenAI-compatible Chat Completions API (/v1/chat/completions),
// GitHub Copilot Extension endpoint (/copilot/agent), MCP Server (/mcp),
// Cluster Diagnostic Ingestion API (/v1/diagnostics/ingest),
// Autonomous Cluster Scan & Incidents API (/v1/cluster/scan, /v1/incidents),
// Multi-Cluster Hub & Spoke Agent API (/v1/hub/clusters, /v1/hub/clusters/{id}/commands),
// Operator Custom Resource Adapters & Hierarchy API (/v1/adapters, /v1/hierarchy),
// Prometheus Alertmanager Webhook Receiver (/v1/webhooks/alertmanager),
// and Approval API (/v1/approvals).
type Server struct {
	llmEngine         *llm.Engine
	remediationEngine *remediation.Engine
	gitopsManager     *gitops.Manager
	k8sClient         *k8s.Client
	mcpServer         *mcp.Server
	clusterWatcher    *watcher.Watcher
	hubManager        *hub.Manager
	mux               *http.ServeMux
}

// NewServer constructs and wires all routes for kube-doctor.
func NewServer(
	llmEngine *llm.Engine,
	remEngine *remediation.Engine,
	gitopsMgr *gitops.Manager,
	k8sClient *k8s.Client,
	mcpServer *mcp.Server,
) *Server {
	s := &Server{
		llmEngine:         llmEngine,
		remediationEngine: remEngine,
		gitopsManager:     gitopsMgr,
		k8sClient:         k8sClient,
		mcpServer:         mcpServer,
		mux:               http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

// SetWatcher attaches the autonomous background cluster health watcher and Alertmanager webhook processor.
func (s *Server) SetWatcher(w *watcher.Watcher) {
	s.clusterWatcher = w
	if s.mcpServer != nil {
		s.mcpServer.SetWatcher(w)
	}
}

// SetHubManager attaches the Multi-Cluster Hub/Agent Fleet Manager.
func (s *Server) SetHubManager(hm *hub.Manager) {
	s.hubManager = hm
	if s.mcpServer != nil {
		s.mcpServer.SetHubAndK8sClient(hm, s.k8sClient)
	}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-GitHub-Token, X-Copilot-Integration-Id, Mcp-Session-Id, Accept")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) registerRoutes() {
	s.mux.HandleFunc("/healthz", s.handleHealthz)
	s.mux.HandleFunc("/v1/models", s.handleModels)
	s.mux.HandleFunc("/v1/chat/completions", s.handleChatCompletions)
	s.mux.HandleFunc("/copilot/agent", s.handleCopilotExtension)
	s.mux.HandleFunc("/v1/diagnostics/ingest", s.handleDiagnosticsIngest)
	s.mux.HandleFunc("/v1/cluster/scan", s.handleClusterScan)
	s.mux.HandleFunc("/v1/incidents", s.handleIncidentsList)
	s.mux.HandleFunc("/v1/webhooks/alertmanager", s.handleAlertmanagerWebhook)
	s.mux.HandleFunc("/v1/approvals", s.handleApprovalsList)
	s.mux.HandleFunc("/v1/approvals/", s.handleApprovalAction)
	s.mux.HandleFunc("/v1/audit", s.handleAuditLog)
	s.mux.HandleFunc("/v1/adapters", s.handleAdapters)
	s.mux.HandleFunc("/v1/hierarchy", s.handleHierarchyInspect)
	s.mux.HandleFunc("/v1/hub/clusters", s.handleHubClusters)
	s.mux.HandleFunc("/v1/hub/clusters/", s.handleHubClusterSubroutes)
	s.mux.Handle("/mcp", s.mcpServer)
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	var watcherStatus any
	if s.clusterWatcher != nil {
		watcherStatus = s.clusterWatcher.Status()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                 "healthy",
		"model":                  llm.DefaultModelName,
		"supportedKubernetesMin": "1.35.0",
		"supportedOpenShiftMin":  "4.22.0",
		"watcher":                watcherStatus,
		"endpoints": []string{
			"GET  /v1/models",
			"POST /v1/chat/completions",
			"POST /copilot/agent",
			"POST /mcp",
			"POST /v1/cluster/scan",
			"GET  /v1/incidents",
			"POST /v1/webhooks/alertmanager",
			"POST /v1/diagnostics/ingest",
			"GET  /v1/approvals",
			"POST /v1/approvals/{id}/approve",
			"POST /v1/approvals/{id}/reject",
			"GET  /v1/audit",
		},
	})
}

func (s *Server) handleModels(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"object": "list",
		"data": []map[string]any{
			{
				"id":       llm.DefaultModelName,
				"object":   "model",
				"created":  1760000000,
				"owned_by": "kube-doctor",
				"metadata": map[string]any{
					"kubernetes_version": ">=1.35",
					"openshift_version":  ">=4.22",
					"capabilities":       []string{"tool_calls", "mcp_callbacks", "web_search_callbacks", "autonomous_remediation", "gitops_approvals"},
				},
			},
		},
	})
}

func (s *Server) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req types.ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid request payload: %v", err), http.StatusBadRequest)
		return
	}

	resp, err := s.llmEngine.ChatCompletion(r.Context(), req)
	if err != nil {
		http.Error(w, fmt.Sprintf("inference error: %v", err), http.StatusInternalServerError)
		return
	}

	if req.Stream {
		s.writeStreamingResponse(w, resp)
		return
	}

	writeJSON(w, http.StatusOK, resp)
}

// handleCopilotExtension implements the GitHub Copilot Extension Agent SSE protocol.
// Any Copilot agent can invoke /copilot/agent, receive tool_calls requesting MCP/web info,
// or stream cluster remediation reports directly in GitHub Copilot Chat.
func (s *Server) handleCopilotExtension(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req types.ChatCompletionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("invalid copilot payload: %v", err), http.StatusBadRequest)
		return
	}
	req.Stream = true

	resp, err := s.llmEngine.ChatCompletion(r.Context(), req)
	if err != nil {
		http.Error(w, fmt.Sprintf("copilot agent error: %v", err), http.StatusInternalServerError)
		return
	}

	s.writeStreamingResponse(w, resp)
}

func (s *Server) writeStreamingResponse(w http.ResponseWriter, resp *types.ChatCompletionResponse) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, _ := w.(http.Flusher)

	choice := resp.Choices[0]
	chunk := map[string]any{
		"id":      resp.ID,
		"object":  "chat.completion.chunk",
		"created": resp.Created,
		"model":   resp.Model,
		"choices": []map[string]any{
			{
				"index": 0,
				"delta": map[string]any{
					"role":       choice.Message.Role,
					"content":    choice.Message.Content,
					"tool_calls": choice.Message.ToolCalls,
				},
				"finish_reason": choice.FinishReason,
			},
		},
	}
	chunkBytes, _ := json.Marshal(chunk)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", string(chunkBytes))
	_, _ = fmt.Fprintf(w, "data: [DONE]\n\n")
	if flusher != nil {
		flusher.Flush()
	}
}

func (s *Server) handleDiagnosticsIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var ev types.DiagnosticEvent
	if err := json.NewDecoder(r.Body).Decode(&ev); err != nil {
		http.Error(w, fmt.Sprintf("invalid diagnostic event: %v", err), http.StatusBadRequest)
		return
	}
	if ev.Timestamp.IsZero() {
		ev.Timestamp = time.Now().UTC()
	}

	assessment, err := s.remediationEngine.EvaluateAndAct(r.Context(), ev)
	if err != nil {
		http.Error(w, fmt.Sprintf("remediation evaluation failed: %v", err), http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, assessment)
}

func (s *Server) handleApprovalsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"approvals": s.gitopsManager.ListApprovals(),
	})
}

func (s *Server) handleApprovalAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Expected path: /v1/approvals/{id}/approve or /v1/approvals/{id}/reject
	trimmed := strings.TrimPrefix(r.URL.Path, "/v1/approvals/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 2 {
		http.Error(w, "expected /v1/approvals/{id}/approve or /v1/approvals/{id}/reject", http.StatusBadRequest)
		return
	}
	approvalID := parts[0]
	action := parts[1]

	var body struct {
		ApprovedBy string `json:"approvedBy"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.ApprovedBy == "" {
		body.ApprovedBy = "cluster-admin"
	}

	switch action {
	case "approve":
		req, err := s.gitopsManager.ApproveAndExecute(r.Context(), approvalID, body.ApprovedBy, s.remediationEngine.ExecuteApprovedAction)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error(), "approvalId": approvalID})
			return
		}
		writeJSON(w, http.StatusOK, req)

	case "reject":
		req, err := s.gitopsManager.Reject(approvalID, body.ApprovedBy)
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error(), "approvalId": approvalID})
			return
		}
		writeJSON(w, http.StatusOK, req)

	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("unknown approval action %q", action)})
	}
}

func (s *Server) handleClusterScan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s.clusterWatcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "cluster watcher not configured"})
		return
	}
	force := r.URL.Query().Get("force") == "true"
	var body struct {
		Force bool `json:"force"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Force {
		force = true
	}

	records, err := s.clusterWatcher.ScanAndRemediate(r.Context(), "ON_DEMAND_SCAN", force)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"watcherStatus":    s.clusterWatcher.Status(),
		"newlyRemediated":  records,
		"allIncidents":     s.clusterWatcher.ListIncidents(),
		"pendingApprovals": s.gitopsManager.ListApprovals(),
	})
}

func (s *Server) handleIncidentsList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s.clusterWatcher == nil {
		writeJSON(w, http.StatusOK, map[string]any{"incidents": []any{}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"watcherStatus":    s.clusterWatcher.Status(),
		"incidents":        s.clusterWatcher.ListIncidents(),
		"pendingApprovals": s.gitopsManager.ListApprovals(),
	})
}

func (s *Server) handleAlertmanagerWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	if s.clusterWatcher == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "cluster watcher not configured"})
		return
	}
	var payload watcher.AlertmanagerPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": fmt.Sprintf("invalid Alertmanager webhook payload: %v", err)})
		return
	}
	records, err := s.clusterWatcher.HandleAlertmanagerWebhook(r.Context(), payload)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":         "processed",
		"alertsReceived": len(payload.Alerts),
		"incidents":      records,
	})
}

func (s *Server) handleAuditLog(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"auditEntries": s.k8sClient.AuditEntries(),
	})
}

func (s *Server) handleAdapters(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var spec adapters.DeclarativeAdapterSpec
		if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		if err := s.remediationEngine.AdapterRegistry().RegisterDeclarativeSpec(spec); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{
			"status":   "registered",
			"adapter":  spec,
			"adapters": s.remediationEngine.AdapterRegistry().ListMetadata(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"adapters": s.remediationEngine.AdapterRegistry().ListMetadata(),
	})
}

func (s *Server) handleHierarchyInspect(w http.ResponseWriter, r *http.Request) {
	ns := r.URL.Query().Get("namespace")
	if ns == "" {
		ns = "default"
	}
	apiVer := r.URL.Query().Get("apiVersion")
	kind := r.URL.Query().Get("kind")
	if kind == "" {
		kind = "Pod"
	}
	name := r.URL.Query().Get("name")
	if name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "query parameter 'name' is required"})
		return
	}
	tree, err := s.k8sClient.ResolveOwnershipHierarchy(r.Context(), ns, apiVer, kind, name)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tree)
}

func (s *Server) handleHubClusters(w http.ResponseWriter, r *http.Request) {
	if s.hubManager == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "hub manager not initialized"})
		return
	}
	if r.Method == http.MethodPost {
		var hb hub.HeartbeatPayload
		if err := json.NewDecoder(r.Body).Decode(&hb); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		cluster, newIncs, err := s.hubManager.RegisterOrHeartbeat(r.Context(), hb)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"cluster":         cluster,
			"newIncidents":    newIncs,
			"pendingCommands": s.hubManager.PendingCommands(hb.ClusterID),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"mode":     s.hubManager.Mode(),
		"clusters": s.hubManager.ListClusters(),
	})
}

func (s *Server) handleHubClusterSubroutes(w http.ResponseWriter, r *http.Request) {
	if s.hubManager == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"error": "hub manager not initialized"})
		return
	}
	trimmed := strings.TrimPrefix(r.URL.Path, "/v1/hub/clusters/")
	if trimmed == "register" || trimmed == "heartbeat" {
		s.handleHubClusters(w, r)
		return
	}
	parts := strings.Split(trimmed, "/")
	clusterID := parts[0]
	if len(parts) == 1 {
		writeJSON(w, http.StatusOK, map[string]any{
			"clusterId":       clusterID,
			"incidents":       s.hubManager.ListFleetIncidents(clusterID),
			"pendingCommands": s.hubManager.PendingCommands(clusterID),
		})
		return
	}
	sub := parts[1]
	switch sub {
	case "commands":
		if len(parts) >= 4 && parts[3] == "ack" && r.Method == http.MethodPost {
			cmdID := parts[2]
			var ackBody struct {
				Status        string `json:"status"`
				ResultMessage string `json:"resultMessage"`
			}
			_ = json.NewDecoder(r.Body).Decode(&ackBody)
			cmd, err := s.hubManager.AckCommand(clusterID, cmdID, ackBody.Status, ackBody.ResultMessage)
			if err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, cmd)
			return
		}
		if r.Method == http.MethodPost {
			var act types.RemediationAction
			if err := json.NewDecoder(r.Body).Decode(&act); err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			cmd, err := s.hubManager.DispatchCommand(r.Context(), clusterID, act)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
				return
			}
			writeJSON(w, http.StatusCreated, cmd)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"clusterId": clusterID,
			"commands":  s.hubManager.PendingCommands(clusterID),
		})
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": fmt.Sprintf("unknown hub subroute %q", sub)})
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

