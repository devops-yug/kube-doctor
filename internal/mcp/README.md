# `internal/mcp` — Native Integration with `containers/kubernetes-mcp-server`

`kube-doctor` integrates natively with [`github.com/containers/kubernetes-mcp-server`](https://github.com/containers/kubernetes-mcp-server) in **two ways**:

---

## 1. Direct Backend / Sidecar MCP Client ([`internal/mcp/k8s_mcp_client.go`](internal/mcp/k8s_mcp_client.go))

- Connects to `kubernetes-mcp-server` over **Streamable HTTP** (`K8S_MCP_SERVER_URL=http://127.0.0.1:8081/mcp`) or **Stdio** (`K8S_MCP_SERVER_COMMAND="npx -y kubernetes-mcp-server@latest"`).
- Automatically invokes:
  - `pods_get` & `pods_log` (`previous=true`) to inspect `OOMKilled` (`ExitCode 137`) and `CrashLoopBackOff` pods.
  - `nodes_stats_summary` to read Kubernetes 1.35+ **cgroup v2 PSI (Pressure Stall Information)** memory pressure telemetry.
  - `resources_get` to inspect `StatefulSet`, `Deployment`, Operator Custom Resources, `ResourceQuota`, and OpenShift 4.22 `MachineSet` (`machine.openshift.io/v1beta1`) specs.
  - `resources_create_or_update` (Server-Side Apply) to update workload/CR memory requests and limits, create `RemediationRequest` CRDs, and apply approved `ResourceQuota` expansions.
  - `resources_scale` to autoscale OpenShift 4.22 `MachineSet` replicas once approved.

### Running Locally with `containers/kubernetes-mcp-server`

```bash
# Option A: Streamable HTTP mode (used by the sidecar in Kubernetes/OpenShift deployments)
kubernetes-mcp-server --config deploy/mcp-config.toml &
export K8S_MCP_SERVER_URL="http://127.0.0.1:8081/mcp"
go run ./cmd/kube-doctor

# Option B: Stdio mode via npx
export K8S_MCP_SERVER_COMMAND="npx -y kubernetes-mcp-server@latest"
go run ./cmd/kube-doctor
```

---

## 2. Copilot & IDE Agent Tool-Calling ([`internal/llm/engine.go`](internal/llm/engine.go) & [`internal/mcp/server.go`](internal/mcp/server.go))

- **Unified MCP Server (`POST /mcp`)**:
  - `kube-doctor` exposes its own SRE remediation tools (`scan_and_remediate_cluster_health`, `list_active_incidents`, `list_managed_clusters`, `inspect_resource_hierarchy`, `list_operator_adapters`, `verify_remediation_effectiveness`, `diagnose_and_remediate_cluster_issue`, `approve_remediation_request`, `query_k8s_135_ocp_422_knowledge`) **plus** all exported `containers/kubernetes-mcp-server` tools (`pods_get`, `pods_log`, `pods_top`, `events_list`, `nodes_stats_summary`, `resources_get`, `resources_create_or_update`, `resources_scale`, etc.) on a single MCP endpoint.
- **OpenAI Tool-Calling (`POST /v1/chat/completions` & `POST /copilot/agent`)**:
  - When an external Copilot Extension, VS Code Copilot Chat, Cursor, or Claude Code agent calls `/v1/chat/completions` with `containers/kubernetes-mcp-server` tools registered, `kube-doctor` emits standard OpenAI `tool_calls` for those tools alongside web search tools, then consumes the agent's `role: "tool"` output on the next turn.
