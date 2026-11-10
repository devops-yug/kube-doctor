package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/hub"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
	"github.com/luigi/kube-doctor/internal/watcher"
)

// Server implements a Model Context Protocol (MCP) JSON-RPC 2.0 endpoint
// that exposes kube-doctor's severity/approval remediation tools, Multi-Cluster Hub tools,
// Operator Custom Resource Adapter tools, AND bridges directly to https://github.com/containers/kubernetes-mcp-server.
type Server struct {
	ragEngine         *rag.Engine
	remediationEngine *remediation.Engine
	gitopsManager     *gitops.Manager
	k8sMCPClient      *K8sMCPClient
	clusterWatcher    *watcher.Watcher
	hubManager        *hub.Manager
	k8sClient         *k8s.Client
}

// NewServer creates a new MCP Server with optional K8sMCPClient bridge.
func NewServer(ragEngine *rag.Engine, remEngine *remediation.Engine, gitopsMgr *gitops.Manager, k8sMCP ...*K8sMCPClient) *Server {
	var client *K8sMCPClient
	if len(k8sMCP) > 0 && k8sMCP[0] != nil {
		client = k8sMCP[0]
	} else {
		client = NewK8sMCPClient()
	}
	return &Server{
		ragEngine:         ragEngine,
		remediationEngine: remEngine,
		gitopsManager:     gitopsMgr,
		k8sMCPClient:      client,
	}
}

// SetWatcher attaches the autonomous cluster health watcher to the MCP server.
func (s *Server) SetWatcher(w *watcher.Watcher) {
	s.clusterWatcher = w
}

// SetHubAndK8sClient attaches the Multi-Cluster Hub Manager and Kubernetes REST Client.
func (s *Server) SetHubAndK8sClient(hm *hub.Manager, kc *k8s.Client) {
	s.hubManager = hm
	s.k8sClient = kc
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id"`
	Result  any    `json:"result,omitempty"`
	Error   any    `json:"error,omitempty"`
}

// ServeHTTP handles MCP Streamable HTTP & JSON-RPC 2.0 requests
// (initialize, notifications/initialized, ping, tools/list, tools/call, prompts/list, resources/list).
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Handle DELETE /mcp (session close in Streamable HTTP)
	if r.Method == http.MethodDelete {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Handle GET /mcp (optional server-to-client SSE stream in Streamable HTTP)
	// Returning 405 Method Not Allowed tells Streamable HTTP clients (like VS Code)
	// that this stateless server does not require a standalone GET SSE listener.
	if r.Method == http.MethodGet {
		http.Error(w, "standalone SSE stream not required for stateless MCP server", http.StatusMethodNotAllowed)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req jsonRPCRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			Error:   map[string]any{"code": -32700, "message": "parse error"},
		})
		return
	}

	// Per MCP Streamable HTTP specification: JSON-RPC notifications (no "id" field,
	// e.g. "notifications/initialized", "notifications/cancelled") MUST return 202 Accepted with no body.
	if req.ID == nil || (len(req.Method) >= 14 && req.Method[:14] == "notifications/") {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	switch req.Method {
	case "initialize", "server/discover":
		protocolVersion := "2024-11-05"
		var initParams struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if err := json.Unmarshal(req.Params, &initParams); err == nil && initParams.ProtocolVersion != "" {
			protocolVersion = initParams.ProtocolVersion
		}
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"protocolVersion": protocolVersion,
				"serverInfo": map[string]any{
					"name":    "kube-doctor-mcp",
					"version": "1.35.0-ocp4.22",
				},
				"capabilities": map[string]any{
					"tools": map[string]any{
						"listChanged": false,
					},
				},
				"tools": s.toolDefinitions(),
			},
		})

	case "ping":
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{},
		})

	case "tools/list":
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"tools": s.toolDefinitions(),
			},
		})

	case "prompts/list":
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"prompts": []any{},
			},
		})

	case "resources/list":
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"resources": []any{},
			},
		})

	case "resources/templates/list":
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: map[string]any{
				"resourceTemplates": []any{},
			},
		})

	case "logging/setLevel":
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  map[string]any{},
		})

	case "tools/call":
		res, err := s.handleToolCall(r.Context(), req.Params)
		if err != nil {
			writeJSON(w, jsonRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Error:   map[string]any{"code": -32000, "message": err.Error()},
			})
			return
		}
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result:  res,
		})

	default:
		writeJSON(w, jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   map[string]any{"code": -32601, "message": fmt.Sprintf("unknown MCP method %q", req.Method)},
		})
	}
}

func (s *Server) toolDefinitions() []map[string]any {
	tools := []map[string]any{
		{
			"name":        "scan_and_remediate_cluster_health",
			"description": "Autonomously scan the live Kubernetes/OpenShift cluster right now to discover any unhealthy Pods (OOMKilled, CrashLoopBackOff, ImagePullBackOff, Evicted), Nodes (NodeNotReady), PVCs (Pending), and Namespace ResourceQuotas directly from the Kubernetes API server, and automatically execute or stage remediation.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"forceReevaluate": map[string]any{
						"type":        "boolean",
						"description": "If true, re-evaluates currently unhealthy resources even if they were already processed in the last 5 minutes.",
					},
				},
			},
		},
		{
			"name":        "list_active_incidents",
			"description": "List all cluster incidents autonomously discovered by the background cluster watcher, Spoke Cluster Agents, or Prometheus Alertmanager webhooks, including their triage assessments, operator ownership chains, and pending approval IDs.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"clusterId": map[string]any{
						"type":        "string",
						"description": "Optional cluster ID filter when managing multiple clusters.",
					},
				},
			},
		},
		{
			"name":        "list_managed_clusters",
			"description": "List all Kubernetes >= 1.35 and OpenShift >= 4.22 clusters registered with the kube-doctor Multi-Cluster Hub, including their health status, active incident counts, and installed operator adapters.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "inspect_resource_hierarchy",
			"description": "Walk the full multi-hop metadata.ownerReferences and Operator Custom Resource hierarchy (up to 8 levels deep, e.g. Pod -> StatefulSet -> NooBaa -> BackingStore/BucketClass -> StorageCluster, or Pod -> Cluster.postgresql.cnpg.io) and return the complete parent/child CRD topology and status conditions.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"namespace":  map[string]any{"type": "string"},
					"apiVersion": map[string]any{"type": "string"},
					"kind":       map[string]any{"type": "string"},
					"name":       map[string]any{"type": "string"},
				},
				"required": []string{"name"},
			},
		},
		{
			"name":        "list_operator_adapters",
			"description": "List all registered Operator Custom Resource Adapters (built-in Go adapters like cloudnative-pg, noobaa-odf, strimzi-kafka and declarative YAML profiles like percona-pg) and their supported GVKs.",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "verify_remediation_effectiveness",
			"description": "Run the Post-Remediation Effectiveness Monitor across all remediated incidents to verify whether target Pods/CustomResources recovered (Ready=True), check for post-fix container restarts, detect spec drift, and compute Effectiveness Scores (0-100).",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        "diagnose_and_remediate_cluster_issue",
			"description": "Diagnose a Kubernetes >= 1.35 / OpenShift >= 4.22 cluster event (e.g. OOMKilled StatefulSet pod, Namespace ResourceQuota exceeded, or missing OpenShift MachineSet node) and execute or stage corrective action based on criticality and severity.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"namespace":            map[string]any{"type": "string"},
					"podName":              map[string]any{"type": "string"},
					"ownerKind":            map[string]any{"type": "string"},
					"ownerName":            map[string]any{"type": "string"},
					"exitCode":             map[string]any{"type": "integer"},
					"reason":               map[string]any{"type": "string"},
					"message":              map[string]any{"type": "string"},
					"currentMemoryLimit":   map[string]any{"type": "string"},
					"currentMemoryRequest": map[string]any{"type": "string"},
					"quotaName":            map[string]any{"type": "string"},
					"quotaMemoryLimit":     map[string]any{"type": "string"},
					"quotaMemoryUsed":      map[string]any{"type": "string"},
					"machineSetName":       map[string]any{"type": "string"},
					"expectedMachines":     map[string]any{"type": "integer"},
					"readyMachines":        map[string]any{"type": "integer"},
				},
				"required": []string{"reason"},
			},
		},
		{
			"name":        "approve_remediation_request",
			"description": "Approve a pending RemediationRequest CRD and GitOps Pull Request (for Namespace ResourceQuota expansion or OpenShift 4.22 MachineSet autoscaling) and execute the cluster mutation.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"approvalId": map[string]any{"type": "string"},
					"approvedBy": map[string]any{"type": "string"},
				},
				"required": []string{"approvalId"},
			},
		},
		{
			"name":        "query_k8s_135_ocp_422_knowledge",
			"description": "Query the specialized Kubernetes >= 1.35 and OpenShift >= 4.22 RAG knowledge base.",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"query": map[string]any{"type": "string"},
				},
				"required": []string{"query"},
			},
		},
	}
	// Append all github.com/containers/kubernetes-mcp-server tools
	tools = append(tools, ExportedToolSchemas()...)
	return tools
}

func (s *Server) handleToolCall(ctx context.Context, rawParams json.RawMessage) (any, error) {
	var call struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(rawParams, &call); err != nil {
		return nil, err
	}

	switch call.Name {
	case "scan_and_remediate_cluster_health":
		if s.clusterWatcher == nil {
			return nil, fmt.Errorf("cluster watcher is not initialized")
		}
		var args struct {
			ForceReevaluate bool `json:"forceReevaluate"`
		}
		_ = json.Unmarshal(call.Arguments, &args)
		records, err := s.clusterWatcher.ScanAndRemediate(ctx, "MCP_ON_DEMAND_SCAN", args.ForceReevaluate)
		if err != nil {
			return nil, err
		}
		payload := map[string]any{
			"watcherStatus":   s.clusterWatcher.Status(),
			"newlyRemediated": records,
			"allIncidents":    s.clusterWatcher.ListIncidents(),
			"pendingApprovals": s.gitopsManager.ListApprovals(),
		}
		outBytes, _ := json.MarshalIndent(payload, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "list_active_incidents":
		var args struct {
			ClusterID string `json:"clusterId"`
		}
		_ = json.Unmarshal(call.Arguments, &args)
		var incidents any
		var status any
		var clusters any
		if s.hubManager != nil && args.ClusterID != "" {
			incidents = s.hubManager.ListFleetIncidents(args.ClusterID)
		} else if s.clusterWatcher != nil {
			incidents = s.clusterWatcher.ListIncidents()
			status = s.clusterWatcher.Status()
		}
		if s.hubManager != nil {
			clusters = s.hubManager.ListClusters()
		}
		payload := map[string]any{
			"watcherStatus":    status,
			"managedClusters":  clusters,
			"incidents":        incidents,
			"pendingApprovals": s.gitopsManager.ListApprovals(),
		}
		outBytes, _ := json.MarshalIndent(payload, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "list_managed_clusters":
		var clusters any
		var mode any = "standalone"
		if s.hubManager != nil {
			clusters = s.hubManager.ListClusters()
			mode = s.hubManager.Mode()
		}
		payload := map[string]any{
			"mode":            mode,
			"managedClusters": clusters,
		}
		outBytes, _ := json.MarshalIndent(payload, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "inspect_resource_hierarchy":
		var args struct {
			Namespace  string `json:"namespace"`
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
		}
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, err
		}
		if args.Namespace == "" {
			args.Namespace = "default"
		}
		if args.Kind == "" {
			args.Kind = "Pod"
		}
		if s.k8sClient == nil {
			return nil, fmt.Errorf("k8s client is not initialized")
		}
		tree, err := s.k8sClient.ResolveOwnershipHierarchy(ctx, args.Namespace, args.APIVersion, args.Kind, args.Name)
		if err != nil {
			return nil, err
		}
		outBytes, _ := json.MarshalIndent(tree, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "list_operator_adapters":
		var adaptersMeta any
		if s.remediationEngine != nil && s.remediationEngine.AdapterRegistry() != nil {
			adaptersMeta = s.remediationEngine.AdapterRegistry().ListMetadata()
		}
		payload := map[string]any{
			"operatorAdapters": adaptersMeta,
		}
		outBytes, _ := json.MarshalIndent(payload, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "verify_remediation_effectiveness":
		var incidents any
		var status any
		if s.clusterWatcher != nil {
			incidents = s.clusterWatcher.VerifyRemediationEffectiveness(ctx)
			status = s.clusterWatcher.Status()
		}
		payload := map[string]any{
			"watcherStatus": status,
			"incidents":     incidents,
		}
		outBytes, _ := json.MarshalIndent(payload, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "diagnose_and_remediate_cluster_issue":
		var ev types.DiagnosticEvent
		if err := json.Unmarshal(call.Arguments, &ev); err != nil {
			return nil, err
		}
		assessment, err := s.remediationEngine.EvaluateAndAct(ctx, ev)
		if err != nil {
			return nil, err
		}
		outBytes, _ := json.MarshalIndent(assessment, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "approve_remediation_request":
		var args struct {
			ApprovalID string `json:"approvalId"`
			ApprovedBy string `json:"approvedBy"`
		}
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, err
		}
		if args.ApprovedBy == "" {
			args.ApprovedBy = "mcp-copilot-admin"
		}
		req, err := s.gitopsManager.ApproveAndExecute(ctx, args.ApprovalID, args.ApprovedBy, s.remediationEngine.ExecuteApprovedAction)
		if err != nil {
			return nil, err
		}
		outBytes, _ := json.MarshalIndent(req, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	case "query_k8s_135_ocp_422_knowledge":
		var args struct {
			Query string `json:"query"`
		}
		if err := json.Unmarshal(call.Arguments, &args); err != nil {
			return nil, err
		}
		docs := s.ragEngine.Retrieve(args.Query, 3)
		outBytes, _ := json.MarshalIndent(docs, "", "  ")
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": string(outBytes)},
			},
		}, nil

	// Forward all github.com/containers/kubernetes-mcp-server tools to K8sMCPClient
	case ToolEventsList, ToolNamespacesList, ToolProjectsList, ToolNodesLog,
		ToolNodesStatsSummary, ToolNodesTop, ToolPodsList, ToolPodsListInNamespace,
		ToolPodsGet, ToolPodsDelete, ToolPodsTop, ToolPodsExec, ToolPodsLog,
		ToolResourcesList, ToolResourcesGet, ToolResourcesCreateOrUpdate,
		ToolResourcesDelete, ToolResourcesScale:
		var argMap map[string]any
		_ = json.Unmarshal(call.Arguments, &argMap)
		resText, err := s.k8sMCPClient.CallTool(ctx, call.Name, argMap)
		if err != nil {
			return nil, err
		}
		if call.Name == ToolEventsList && s.clusterWatcher != nil {
			_, _ = s.clusterWatcher.ScanAndRemediate(ctx, "MCP_EVENTS_SCAN", false)
			summary := map[string]any{
				"kubeDoctorWatcherStatus": s.clusterWatcher.Status(),
				"kubeDoctorIncidents":     s.clusterWatcher.ListIncidents(),
				"pendingApprovals":        s.gitopsManager.ListApprovals(),
			}
			if sumBytes, mErr := json.MarshalIndent(summary, "", "  "); mErr == nil {
				resText = "# kube-doctor Active Incidents & Autonomous Remediation Summary:\n" + string(sumBytes) + "\n\n" + resText
			}
		}
		return map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": resText},
			},
		}, nil

	default:
		return nil, fmt.Errorf("unknown tool %q", call.Name)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
