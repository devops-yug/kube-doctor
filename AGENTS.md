# AGENTS.md — `kube-doctor` Architecture, Agent Instructions & Engineering Rationale

Welcome to **`kube-doctor`** (`kube-doctor-1.35-ocp-4.22`), an autonomous Site Reliability Engineering (SRE) LLM, RAG engine, Model Context Protocol (MCP) server/client, Multi-Cluster Fleet Control Plane, and Operator Custom Resource Remediation Controller purpose-built for **Kubernetes `>= 1.35`** and **Red Hat OpenShift `>= 4.22`**.

This file provides AI coding agents (GitHub Copilot, Claude, Cursor, Antigravity, and custom MCP agents) and human contributors with:
1. **System Architecture & Component Map**
2. **Architectural Decisions ("Why We Built Things This Way")**
3. **Strict Operational & Governance Rules for Agents**
4. **Development, Testing, and Live Cluster Verification Workflows**

---

## 1. System Architecture & Codebase Map

```text
kube-doctor/
├── cmd/kube-doctor/main.go            # Single-binary entrypoint (--mode=standalone|hub|agent)
├── internal/
│   ├── types/types.go                 # 27 IssueTypes, DiagnosticEvent, OwnerChainLink, TriageAssessment
│   ├── k8s/
│   │   ├── client.go                  # K8s 1.35+ / OCP 4.22+ REST client (pods/resize, generic CR patching)
│   │   ├── scanner.go                 # Live cluster health scanner (Pods, Nodes, PVCs, Quotas, Operator CRs)
│   │   └── hierarchy.go               # Recursive 8-hop metadata.ownerReferences & sibling CR lineage walker
│   ├── adapters/
│   │   ├── registry.go                # Hybrid Operator Adapter Registry (built-in Go + declarative YAML)
│   │   ├── builtin.go                 # Built-in adapters: CloudNativePG, NooBaa/ODF 5-CRD, Strimzi Kafka
│   │   └── adapters_test.go           # Unit tests for CR lineage, CNPG, NooBaa 5-CR, and Hub/Spoke
│   ├── hub/
│   │   └── manager.go                 # Multi-Cluster Hub & Spoke Agent Fleet Manager
│   ├── remediation/
│   │   └── engine.go                  # Criticality/Severity triage & AUTO_REMEDIATE vs APPROVAL_REQUIRED engine
│   ├── gitops/
│   │   └── manager.go                 # RemediationRequest CRD (kubedoctor.io/v1alpha1) & GitOps PR manager
│   ├── llm/
│   │   └── engine.go                  # Specialized LLM orchestrator (Ollama/vLLM upstream + RAG + MCP telemetry)
│   ├── rag/
│   │   └── engine.go                  # K8s >= 1.35 & OpenShift >= 4.22 runbook retrieval engine
│   ├── watcher/
│   │   └── watcher.go                 # 15s autonomous background reconciliation loop + Alertmanager v4 webhook
│   ├── mcp/
│   │   ├── server.go                  # JSON-RPC 2.0 / Streamable HTTP MCP Server (/mcp)
│   │   └── k8s_mcp_client.go          # Client bridge to github.com/containers/kubernetes-mcp-server
│   └── api/
│       └── server.go                  # HTTP Gateway (/v1/chat/completions, /copilot/agent, /mcp, /v1/hub/*)
├── configs/adapters/                  # Declarative YAML Operator Adapter definitions (CNPG, NooBaa, Percona)
├── knowledge/k8s_1_35_ocp_4_22.json   # Authoritative K8s 1.35+ & OpenShift 4.22+ RAG knowledge base
├── scripts/
│   ├── mcp_stdio_bridge.py            # Self-healing Stdio-to-HTTP MCP bridge for VS Code & copilot-cli
│   └── llm_playground.py              # Interactive REPL & scenario runner for the Ollama LLM
├── training/                          # QLoRA fine-tuning pipeline (Axolotl, PEFT/TRL, Ollama Modelfile)
├── deploy/                            # Kubernetes, OpenShift, CRD, and live demo manifests
├── .github/agents/kube-doctor.agent.md# Custom GitHub Copilot Agent definition
├── .mcp.json & .vscode/mcp.json       # Workspace MCP server configurations for VS Code & copilot-cli
└── Makefile                           # Build, test, Podman, kind-load, and OpenShift/K8s deploy targets
```

---

## 2. Why We Built Things a Specific Way (Engineering Rationale)

### 2.1 Why a Stdio-to-HTTP Python Bridge (`scripts/mcp_stdio_bridge.py`) Instead of Direct HTTP in VS Code?
- **Problem Discovered**:
  1. When VS Code runs in Remote-WSL or behind corporate proxy environment variables (`HTTP_PROXY` / `http_proxy`), Node/Electron HTTP clients inside the VS Code extension host often route `localhost:8080` through proxies or hang on long-lived `GET /mcp` SSE streams.
  2. Whenever the `kube-doctor` pod restarts in Kubernetes (e.g., after `kubectl rollout restart`), an existing `kubectl port-forward svc/kube-doctor 8080:8080` process dies, breaking the IDE's MCP connection.
  3. GitHub Copilot's newer `copilot-cli` agent host (`agentHostMain.js`) sends `"method": "server/discover"` (`protocolVersion: "2026-07-28"`) and watches `.mcp.json` in the workspace root requiring `"type": "stdio"`.
- **Why We Built It This Way**:
  - [`scripts/mcp_stdio_bridge.py`](file:///home/luigi/projects/kube-doctor/scripts/mcp_stdio_bridge.py) runs as a fast local `stdio` process that:
    - Responds **instantly (<1ms)** to `initialize`, `server/discover`, and `tools/list` locally so VS Code never times out starting the MCP server even if the cluster is still booting.
    - Bypasses all system proxies (`urllib.request.ProxyHandler({})`) when talking to `127.0.0.1:8080`.
    - **Self-heals `kubectl port-forward`**: `ensure_port_forward()` checks if `127.0.0.1:8080` is reachable before every tool call and automatically spawns/restarts `kubectl -n kube-doctor-system port-forward svc/kube-doctor 8080:8080` in the background if the tunnel dropped.
  - Both [`.mcp.json`](file:///home/luigi/projects/kube-doctor/.mcp.json) and [`.vscode/mcp.json`](file:///home/luigi/projects/kube-doctor/.vscode/mcp.json) are maintained in sync so both the VS Code UI and the `copilot-cli` subagent host attach `kube-doctor` to every chat session.

### 2.2 Why `.github/agents/kube-doctor.agent.md` Omits `tools:` in YAML Frontmatter & Forbids Subagent Delegation
- **Problem Discovered**:
  - In VS Code's `copilot-cli` agent host (`agentHostMain.js`), custom `.agent.md` frontmatter is parsed such that `tools: null` (omitted) grants **all available tools (including all MCP server tools)**, whereas specifying `tools: [kube-doctor/*, web/fetch]` caused `copilot-cli` to filter out MCP tools and leave only `web_fetch`.
  - Additionally, if the agent prompt included the literal string `http://localhost:8080/mcp`, the model attempted to call `web_fetch` on `http://localhost:8080/mcp` (which fails with `405` because `/mcp` is JSON-RPC `POST`) or delegated to a generic read-only subagent (`pod-incident-check`) that lacked MCP tools and fell back to `kubectl` in the terminal.
- **Why We Built It This Way**:
  - [`.github/agents/kube-doctor.agent.md`](file:///home/luigi/projects/kube-doctor/.github/agents/kube-doctor.agent.md) intentionally **omits the `tools:` frontmatter filter** and explicitly instructs the model **never** to delegate cluster checks to background subagents, **never** to run `kubectl` in a terminal, and **never** to `web_fetch` `localhost:8080`.

### 2.3 Why Autonomous Reconciliation + Alertmanager Webhooks + LLM (Instead of Static Rules Only)?
- **Problem Discovered**:
  - Waiting for a human to type pod names, memory limits, and quota values into chat is reactive and slow. Conversely, blind script-based auto-remediation lacks context (pod logs, cgroup v2 PSI metrics, operator CRD status conditions, and RAG runbooks).
- **Why We Built It This Way**:
  - [`internal/watcher/watcher.go`](file:///home/luigi/projects/kube-doctor/internal/watcher/watcher.go) runs a continuous **15-second autonomous control loop** (`ScanAndRemediate`) AND exposes a **Prometheus Alertmanager v4 webhook receiver** (`POST /v1/webhooks/alertmanager`).
  - Whenever an issue is detected, [`internal/k8s/scanner.go`](file:///home/luigi/projects/kube-doctor/internal/k8s/scanner.go) enriches the event with live Pod/Container specs, Owner hierarchy, and Namespace `ResourceQuota` usage, and passes it to [`DiagnoseAndRemediateEvent`](file:///home/luigi/projects/kube-doctor/internal/llm/engine.go#L81-L146) in [`internal/llm/engine.go`](file:///home/luigi/projects/kube-doctor/internal/llm/engine.go).
  - The LLM engine pulls live telemetry from the `containers/kubernetes-mcp-server` sidecar (`pods_get`, `pods_log`, `resources_get`), retrieves K8s 1.35 / OCP 4.22 RAG runbooks, executes or stages the remediation, and queries the local **Ollama** model (`kube-doctor-1.35-ocp-4.22` at `http://127.0.0.1:11434`) to synthesize the root cause analysis.

### 2.4 Why Recursive `ownerReferences` Lineage Walking + Live CRD OpenAPI v3 Schema + AI Field Selection (Zero Hardcoded Go Operators)?
- **Problem Discovered**:
  - Standard Kubernetes controllers assume `Pod -> StatefulSet` or `Pod -> ReplicaSet -> Deployment`.
  - In real enterprise clusters, operators manage resources via Custom Resources (`Cluster.postgresql.cnpg.io`, `NooBaa.noobaa.io`, `Kafka.kafka.strimzi.io`, `PerconaPGCluster.pgv2.percona.com`, etc.), and hardcoding Go structs for each operator does not scale when new operators are installed in a cluster.
- **Why We Built It This Way**:
  1. **Recursive 8-Hop Lineage Walker ([`internal/k8s/hierarchy.go`](file:///home/luigi/projects/kube-doctor/internal/k8s/hierarchy.go))**: `ResolveOwnershipHierarchy` walks `metadata.ownerReferences` up to 8 levels deep to find the **Root Controlling Custom Resource**, and discovers sibling/sub-CRs (`BackingStore`, `BucketClass`, `NamespaceStore`, `Pooler`, `ScheduledBackup`).
  2. **Live CRD OpenAPI v3 Schema Introspection ([`internal/k8s/crd_schema.go`](file:///home/luigi/projects/kube-doctor/internal/k8s/crd_schema.go))**:
     - `InspectCRDSchemaAndInstance` queries the cluster's `/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{plural}.{group}` endpoint AND the live Custom Resource instance (`.spec` and `.status`), extracting all candidate `.spec` field paths, types, and descriptions (or reading optional YAML hints from [`configs/adapters/*.yaml`](file:///home/luigi/projects/kube-doctor/configs/adapters/)).
  3. **AI-Driven CRD Field Selection ([`internal/adapters/builtin.go`](file:///home/luigi/projects/kube-doctor/internal/adapters/builtin.go))**:
     - Instead of hardcoding operator-specific Go structs, `universalAICRDAdapter` passes the **Problem Statement + Live CR `.spec` + CRD OpenAPI v3 Schema Fields** to the **Ollama LLM (`kube-doctor-1.35-ocp-4.22`)** via [`askOllamaForCRDFields`](file:///home/luigi/projects/kube-doctor/internal/adapters/builtin.go#L261-L343) (backed by schema-grounded field matching in [`matchFieldsFromCRDSchema`](file:///home/luigi/projects/kube-doctor/internal/adapters/builtin.go#L347-L428)).
     - The AI model returns the exact dot-path field(s) to update (e.g., `spec.resources.limits.memory`, `spec.storage.size`, `spec.coreResources.limits.memory`), and `kube-doctor` dynamically constructs and applies the JSON Merge Patch!

### 2.5 Why Single-Binary Multi-Cluster Mode (`--mode=standalone | hub | agent`)?
- **Problem Discovered**:
  - Maintaining separate codebases and container images for a central management server and spoke cluster agents creates version drift and deployment friction.
  - Furthermore, managed spoke clusters often sit behind NAT/firewalls where the central management plane cannot directly dial the spoke's Kubernetes API server.
- **Why We Built It This Way**:
  - [`cmd/kube-doctor/main.go`](file:///home/luigi/projects/kube-doctor/cmd/kube-doctor/main.go) and [`internal/hub/manager.go`](file:///home/luigi/projects/kube-doctor/internal/hub/manager.go) use a **single binary and container image** controlled via `--mode` or `KUBE_DOCTOR_MODE`:
    - `standalone` (default): Runs the local watcher, remediation engine, MCP server, and Hub endpoints in one process.
    - `hub`: Central Multi-Cluster Fleet Control Plane (`GET/POST /v1/hub/clusters`), central LLM/RAG triage, central GitOps approvals, and per-cluster command queues.
    - `agent`: Runs inside a spoke cluster, scans local Pods/Nodes/Operator CRDs, pushes heartbeats and `DiagnosticEvent`s outbound to `KUBE_DOCTOR_HUB_URL`, and polls `GET /v1/hub/clusters/{clusterId}/commands` to execute approved/auto-remediated actions locally inside its own cluster boundary.

### 2.6 Why `ctr --namespace=k8s.io images import` in `Makefile` (`make kind-load`)?
- **Problem Discovered**:
  - On Kubernetes `1.36+` `kind` nodes (which use `containerd v2` with config version 4) combined with rootless Podman, `kind load docker-image` fails with a containerd config version check error.
- **Why We Built It This Way**:
  - In [`Makefile`](file:///home/luigi/projects/kube-doctor/Makefile#L51-L55), `make kind-load` pipes `podman save --format docker-archive` directly into `podman exec -i $$node ctr --namespace=k8s.io images import -`, which reliably loads images into `kind` nodes regardless of `containerd` config version.

---

## 3. Governance & Safety Matrix (Criticality & Severity Rules)

All agents and contributors modifying [`internal/remediation/engine.go`](file:///home/luigi/projects/kube-doctor/internal/remediation/engine.go) or [`internal/adapters/`](file:///home/luigi/projects/kube-doctor/internal/adapters/builtin.go) **must** preserve these governance invariants:

| Scenario | Target Resource | Criticality / Severity | Policy | Execution Mechanism |
| :--- | :--- | :--- | :--- | :--- |
| **Within-Quota `StatefulSet` / `Deployment` `OOMKilled` (`ExitCode 137`)** | `StatefulSet` / `Deployment` + `Pod/resize` | `P1-HIGH` / `SEV-2` | `AUTO_REMEDIATE` | Patches workload memory limit/request, invokes K8s 1.35 `pods/{pod}/resize`, and recycles `CrashLoopBackOff` pod |
| **Within-Quota Operator CR `OOMKilled` (e.g. `Cluster.postgresql.cnpg.io`)** | Controlling Operator CR (`Cluster.postgresql.cnpg.io`) | `P1-HIGH` / `SEV-2` | `AUTO_REMEDIATE` | Walks `ownerReferences` and patches CR `.spec.resources.limits.memory` & `.spec.resources.requests.memory` |
| **Multi-CR Operator Degraded (e.g. `NooBaa` 5-CR Hierarchy)** | Authoritative CR (`NooBaa.noobaa.io` / `BackingStore`) | `P1-HIGH` / `SEV-2` | `AUTO_REMEDIATE` | Walks `StorageCluster -> NooBaa -> BackingStore / BucketClass / NamespaceStore` and patches `NooBaa` resources |
| **`OOMKilled` Exceeding Namespace `ResourceQuota`** | `ResourceQuota` first -> then Workload / Operator CR | `P0-CRITICAL` / `SEV-1` | `APPROVAL_REQUIRED` | Creates `RemediationRequest` CRD (`kubedoctor.io/v1alpha1`) + GitOps PR; executes chained actions only on approval |
| **OpenShift 4.22+ Missing Worker Machine (`MachineSet`)** | `MachineSet` (`machine.openshift.io/v1beta1`) | `P0-CRITICAL` / `SEV-1` | `APPROVAL_REQUIRED` | Creates `RemediationRequest` CRD + GitOps PR; scales `MachineSet` via `resources_scale` only on approval |

---

## 4. Developer & Agent Workflows

### 4.1 Build & Run Unit Tests
```bash
make build
make test
```

### 4.2 Test the Specialized Ollama LLM (`kube-doctor-1.35-ocp-4.22`)
Ensure Ollama is running locally (`http://127.0.0.1:11434`):
```bash
# Build/update the Ollama model from training/Modelfile if needed:
make ollama-build

# Run all 5 end-to-end showcase scenarios (StatefulSet OOM, Quota Approval, CloudNativePG CR, NooBaa 5-CR, OpenShift MachineSet):
python3 scripts/llm_playground.py --scenarios

# Or launch the interactive REPL:
python3 scripts/llm_playground.py
```

### 4.3 Deploy & Verify on Live `kind` Cluster
```bash
# Build image with Podman, import into kind containerd, and deploy kube-doctor + kubernetes-mcp-server sidecar:
make kind-load
make deploy-k8s

# Test live self-healing with the OOMKilled StatefulSet demo or CloudNativePG CRD demo:
kubectl apply -f deploy/examples/live-oom-demo.yaml
kubectl apply -f deploy/examples/cnpg-pg-cluster-demo.yaml
```

