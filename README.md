# `kube-doctor` — Autonomous SRE LLM, MCP Server & Remediation Engine for Kubernetes >= 1.35 & OpenShift >= 4.22

`kube-doctor` (`kube-doctor-1.35-ocp-4.22`) is an autonomous Site Reliability Engineering (SRE) LLM gateway, RAG runbook engine, Model Context Protocol (MCP) server/client, multi-cluster fleet control plane, and Operator Custom Resource remediation controller purpose-built for **Kubernetes `>= 1.35`** and **Red Hat OpenShift `>= 4.22`**.

> **Architecture & Internal Documentation:**
> - **System Architecture & Package Map:** [`cmd/kube-doctor/README.md`](./cmd/kube-doctor/README.md)
> - **Native Integration with `containers/kubernetes-mcp-server`:** [`internal/mcp/README.md`](./internal/mcp/README.md)
> - **Engineering Rationale & Agent Governance Rules:** [`AGENTS.md`](./AGENTS.md)

---

## 1. Key Features

### Autonomous Detection & Multi-Hop CRD Remediation
- **15-Second Autonomous Cluster Watcher & Alertmanager Webhook Receiver**: Continuously scans live Pods, Nodes, PVCs, Namespace `ResourceQuota`s, and Operator Custom Resources every 15 seconds, and accepts real-time Prometheus Alertmanager v4 webhooks (`POST /v1/webhooks/alertmanager`).
- **Recursive 8-Hop `ownerReferences` Lineage Discovery**: Walks `metadata.ownerReferences` up to 8 levels deep (`Pod -> ReplicaSet/StatefulSet -> Controlling Custom Resource`) plus sibling/sub-CRs (e.g., CloudNativePG `Cluster -> Pooler/Backup/ScheduledBackup`, OpenShift Data Foundation / NooBaa 5-CRD hierarchy `StorageCluster -> NooBaa -> BackingStore/BucketClass/NamespaceStore`, Strimzi `Kafka`, Percona `PerconaPGCluster`).
- **Zero-Config Live CRD OpenAPI v3 Introspection + AI Field Selection**: Queries `/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{plural}.{group}` and the live CR `.spec`/`.status`, then uses the `kube-doctor-1.35-ocp-4.22` LLM to select and patch the exact Custom Resource dot-path fields (e.g., `spec.resources.limits.memory`, `spec.coreResources.limits.memory`, `spec.storage.size`) rather than fighting child controller reconciliation.
- **Kubernetes 1.35+ & OpenShift 4.22+ Native Capabilities**:
  - **GA In-Place Pod Vertical Scaling (`pods/resize`)**: Resizes container memory/CPU limits in place on Kubernetes `>= 1.35` alongside controller spec updates.
  - **cgroup v2 PSI Telemetry**: Reads node and pod Pressure Stall Information (`nodes_stats_summary`).
  - **OpenShift 4.22+ `MachineSet` Autoscaling**: Diagnoses missing/failed worker nodes and scales `MachineSet` (`machine.openshift.io/v1beta1`) replicas.
- **Post-Remediation Effectiveness Monitor**: Verifies whether remediated Pods and Custom Resources return to `Ready=True`, checks for post-fix container restarts, detects spec drift, and computes an **Effectiveness Score (`0–100`)**.

### Governance, Safety & GitOps Approvals
| Scenario | Target Resource | Criticality / Severity | Policy | Execution Mechanism |
| :--- | :--- | :--- | :--- | :--- |
| **Within-Quota `StatefulSet` / `Deployment` `OOMKilled` (`ExitCode 137`)** | `StatefulSet` / `Deployment` + `Pod/resize` | `P1-HIGH` / `SEV-2` | `AUTO_REMEDIATE` | Patches workload memory limit/request, invokes K8s 1.35 `pods/{pod}/resize`, and recycles `CrashLoopBackOff` pod |
| **Within-Quota Operator CR `OOMKilled` or Degraded** | Controlling Operator CR (`Cluster`, `NooBaa`, etc.) | `P1-HIGH` / `SEV-2` | `AUTO_REMEDIATE` | Walks `ownerReferences`, introspects CRD OpenAPI v3 schema, and patches CR `.spec` resource/storage fields |
| **`OOMKilled` Exceeding Namespace `ResourceQuota`** | `ResourceQuota` first $\rightarrow$ then Workload / Operator CR | `P0-CRITICAL` / `SEV-1` | `APPROVAL_REQUIRED` | Creates `RemediationRequest` CRD (`kubedoctor.io/v1alpha1`) + GitHub PR / GitLab MR; executes chained actions on approval |
| **OpenShift 4.22+ Missing Worker Machine (`MachineSet`)** | `MachineSet` (`machine.openshift.io/v1beta1`) | `P0-CRITICAL` / `SEV-1` | `APPROVAL_REQUIRED` | Creates `RemediationRequest` CRD + GitHub PR / GitLab MR; scales `MachineSet` via `resources_scale` on approval |

---

## 2. Deployment Modes & How to Deploy

`kube-doctor` ships as a **single binary and container image** that runs in three operating modes controlled via `--mode` or `KUBE_DOCTOR_MODE`:

| Mode | Flag / Env | How It Works | When to Use |
| :--- | :--- | :--- | :--- |
| **Standalone** *(Default)* | `--mode=standalone`<br>`KUBE_DOCTOR_MODE=standalone` | Runs the local 15s cluster watcher, remediation engine, LLM/RAG gateway, MCP server (`/mcp`), and OpenShift Console UI (`/ui`) in a single process. | Single Kubernetes `>= 1.35` or OpenShift `>= 4.22` cluster. |
| **Central Hub** | `--mode=hub`<br>`KUBE_DOCTOR_MODE=hub` | Runs the Multi-Cluster Fleet Control Plane (`GET/POST /v1/hub/clusters`). Receives heartbeats and `DiagnosticEvent`s from spoke agents, performs central LLM/RAG triage and GitOps approvals, and queues remediation commands per cluster. | Central management cluster or host managing multiple spoke/edge clusters. |
| **Spoke Agent** | `--mode=agent`<br>`KUBE_DOCTOR_MODE=agent` | Runs inside a remote/edge cluster (including NAT/firewalled clusters or ARM64 Raspberry Pi nodes). Scans local resources, pushes outbound heartbeats to `KUBE_DOCTOR_HUB_URL`, and polls `GET /v1/hub/clusters/{id}/commands` to execute approved actions locally. | Managed spoke clusters or edge clusters that cannot be dialed inbound by the Hub. |

---

### Prerequisites: Build the Local LLM & Container Image

```bash
# 1. Create the specialized Ollama model (kube-doctor-1.35-ocp-4.22)
make ollama-build

# 2. Build the Go binary and run tests
make build
make test

# 3. (Optional) Fine-tune QLoRA weights with PEFT/TRL or Axolotl
make train-qlora
```

---

### Option A: Deploy `standalone` Mode on Kubernetes >= 1.35 (or `kind`)

```bash
# If using kind (Kubernetes 1.35+ / 1.36+): build image and import into kind containerd
make kind-load

# Deploy RemediationRequest CRD, RBAC, kube-doctor, and kubernetes-mcp-server sidecar
make deploy-k8s
```

Or apply the manifests directly with `kubectl`:
```bash
kubectl apply -f deploy/crd/remediationrequest.kubedoctor.io.yaml
kubectl apply -f deploy/kubernetes/deployment.yaml
kubectl -n kube-doctor-system rollout status deployment/kube-doctor
```

---

### Option B: Deploy on Red Hat OpenShift >= 4.22 (with Console Plugin & Route)

Deploys the `RemediationRequest` and `KubeDoctor` CRDs, the `kube-doctor` deployment + `kubernetes-mcp-server` sidecar, the OpenShift `Route`, and the OpenShift Dynamic `ConsolePlugin` (`kube-doctor-console-plugin`):

```bash
make deploy-openshift
```

Enable the plugin in the OpenShift Web Console if not already enabled:
```bash
oc patch console.operator.openshift.io cluster --type=merge \
  -p '{"spec":{"plugins":["kube-doctor-console-plugin"]}}'
```

---

### Option C: Deploy Multi-Cluster `hub` and `agent` Modes

#### 1. Start or Deploy the Central Hub (`--mode=hub`)
On your central management cluster:
```bash
make deploy-hub
# Expose Hub port 8080 to spoke clusters on your LAN/network:
make expose-hub
```
*(Or run the Hub directly as a host process with `make run-hub`.)*

#### 2. Deploy the Spoke Agent (`--mode=agent`) on Each Managed Cluster
In [`deploy/kubernetes/agent-deployment.yaml`](./deploy/kubernetes/agent-deployment.yaml), set `KUBE_DOCTOR_CLUSTER_ID` and point `KUBE_DOCTOR_HUB_URL` to your Hub's address, then deploy:

```bash
# Build for current arch (or `make podman-build-arm64` for ARM64 / Raspberry Pi spoke clusters)
make deploy-agent
```

Verify all registered spoke clusters from the Hub:
```bash
curl -s http://localhost:8080/v1/hub/clusters | jq .
```

---

### Option D: Deploy via Helm Chart

```bash
make helm-install
# Or customize mode and values:
helm upgrade --install kube-doctor ./charts/kube-doctor \
  --namespace kube-doctor-system --create-namespace \
  --set mode=standalone
```

---

## 3. Configure Local Chat Windows & MCP Servers

`kube-doctor` exposes:
- **MCP Server (JSON-RPC 2.0 / Streamable HTTP)**: `POST http://localhost:8080/mcp`
- **Self-Healing Stdio-to-HTTP MCP Bridge**: [`scripts/mcp_stdio_bridge.py`](./scripts/mcp_stdio_bridge.py) (auto-manages `kubectl port-forward svc/kube-doctor 8080:8080`, bypasses corporate proxies, and responds in `<1ms` to IDE discovery).
- **OpenAI-Compatible Chat Completions API**: `POST http://localhost:8080/v1/chat/completions` (`model: "kube-doctor-1.35-ocp-4.22"`)
- **GitHub Copilot Extension Agent Endpoint**: `POST http://localhost:8080/copilot/agent`

### 3.1 VS Code GitHub Copilot (MCP Server + `@kube-doctor` Agent)

This repository includes pre-configured [`.vscode/mcp.json`](./.vscode/mcp.json), [`.mcp.json`](./.mcp.json), and the custom agent definition [`.github/agents/kube-doctor.agent.md`](./.github/agents/kube-doctor.agent.md).

1. **Workspace MCP Configuration ([`.vscode/mcp.json`](./.vscode/mcp.json) & [`.mcp.json`](./.mcp.json))**:
   ```json
   {
     "mcpServers": {
       "kube-doctor": {
         "type": "stdio",
         "command": "python3",
         "args": [
           "/abs/path/to/kube-doctor/scripts/mcp_stdio_bridge.py"
         ],
         "env": {
           "KUBE_DOCTOR_MCP_URL": "http://127.0.0.1:8080/mcp",
           "NO_PROXY": "127.0.0.1,localhost",
           "no_proxy": "127.0.0.1,localhost"
         }
       }
     }
   }
   ```
2. **Use in VS Code Copilot Chat**:
   - Open GitHub Copilot Chat in **Agent Mode** or invoke `@kube-doctor`.
   - All `kube-doctor` MCP tools and `containers/kubernetes-mcp-server` tools are automatically attached, and `scripts/mcp_stdio_bridge.py` will automatically start/heal the `kubectl port-forward` tunnel to the cluster.

---

### 3.2 Cursor & Windsurf IDE

Add `kube-doctor` to `.cursor/mcp.json` (or `~/.cursor/mcp.json` / `~/.codeium/windsurf/mcp_config.json`):

```json
{
  "mcpServers": {
    "kube-doctor": {
      "command": "python3",
      "args": ["/abs/path/to/kube-doctor/scripts/mcp_stdio_bridge.py"],
      "env": {
        "KUBE_DOCTOR_MCP_URL": "http://127.0.0.1:8080/mcp"
      }
    }
  }
}
```

---

### 3.3 Claude Code CLI & Claude Desktop

**Claude Code CLI:**
```bash
claude mcp add kube-doctor -- python3 /abs/path/to/kube-doctor/scripts/mcp_stdio_bridge.py
```

**Claude Desktop (`claude_desktop_config.json`):**
```json
{
  "mcpServers": {
    "kube-doctor": {
      "command": "python3",
      "args": ["/abs/path/to/kube-doctor/scripts/mcp_stdio_bridge.py"],
      "env": {
        "KUBE_DOCTOR_MCP_URL": "http://127.0.0.1:8080/mcp"
      }
    }
  }
}
```

---

### 3.4 Local Interactive Terminal Chat & Scenario Runner (`scripts/llm_playground.py`)

Run an interactive SRE chat session or execute all 5 end-to-end showcase scenarios directly in your terminal:

```bash
# Launch interactive REPL chat with kube-doctor-1.35-ocp-4.22:
python3 scripts/llm_playground.py

# Or run all 5 automated showcase scenarios (StatefulSet OOM, Quota Approval, CloudNativePG CR, NooBaa 5-CR, OpenShift MachineSet):
python3 scripts/llm_playground.py --scenarios
```

---

### 3.5 OpenAI-Compatible Chat UIs (Open WebUI, LibreChat, Continue.dev, AnythingLLM)

Any chat client that supports an OpenAI-compatible API can connect directly to `kube-doctor`:

- **Base URL**: `http://localhost:8080/v1`
- **API Key**: `sk-kube-doctor` *(any non-empty string)*
- **Model ID**: `kube-doctor-1.35-ocp-4.22`

Example `Continue.dev` (`~/.continue/config.json`) configuration:
```json
{
  "models": [
    {
      "title": "Kube-Doctor 1.35 / OCP 4.22",
      "provider": "openai",
      "model": "kube-doctor-1.35-ocp-4.22",
      "apiBase": "http://localhost:8080/v1",
      "apiKey": "sk-kube-doctor"
    }
  ]
}
```

---

## 4. How to Remediate Any Incident

Incidents are remediated **autonomously** every 15 seconds by the background watcher (and on incoming Alertmanager webhooks at `POST /v1/webhooks/alertmanager`), or **on demand** via **REST API**, **Chat Window / MCP**, or the **OpenShift Console UI Plugin**.

---

### Method 1: Remediate via REST API

Ensure `kube-doctor` is reachable on `localhost:8080` (e.g., `kubectl -n kube-doctor-system port-forward svc/kube-doctor 8080:8080`):

#### 1. Trigger a Live Cluster Scan & Auto-Remediation
Scans all namespaces for unhealthy Pods, Nodes, PVCs, `ResourceQuota`s, and Operator CRs, auto-remediating `P1-HIGH`/`SEV-2` issues and staging approvals for `P0-CRITICAL`/`SEV-1` issues:
```bash
curl -s -X POST "http://localhost:8080/v1/cluster/scan?force=true" | jq .
```

#### 2. List Active & Resolved Incidents + Effectiveness Scores
```bash
curl -s "http://localhost:8080/v1/incidents?includeResolved=true" | jq .
curl -s "http://localhost:8080/v1/effectiveness" | jq .
```

#### 3. Inspect Multi-Hop `ownerReferences` & Operator CRD Lineage
```bash
curl -s "http://localhost:8080/v1/hierarchy?namespace=default&kind=Pod&name=pg-cluster-1" | jq .
```

#### 4. Diagnose & Remediate a Specific Incident Payload On-Demand
```bash
curl -s -X POST "http://localhost:8080/v1/diagnostics/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": "default",
    "podName": "pg-cluster-1",
    "ownerApiVersion": "postgresql.cnpg.io/v1",
    "ownerKind": "Cluster",
    "ownerName": "pg-cluster",
    "exitCode": 137,
    "reason": "OOMKilled",
    "currentMemoryLimit": "256Mi",
    "currentMemoryRequest": "128Mi",
    "quotaMemoryLimit": "4Gi",
    "quotaMemoryUsed": "512Mi"
  }' | jq .
```

#### 5. Review, Approve, Reject, or Open a GitOps PR for `P0-CRITICAL` Incidents
When an incident requires human approval (e.g., expanding a Namespace `ResourceQuota` or scaling an OpenShift `MachineSet`):
```bash
# 1. List pending RemediationRequest approvals
curl -s "http://localhost:8080/v1/approvals" | jq .

# 2. Approve and immediately execute the chained cluster mutation(s)
curl -s -X POST "http://localhost:8080/v1/approvals/<APPROVAL_ID>/approve" \
  -H "Content-Type: application/json" \
  -d '{"approvedBy": "sre-admin"}' | jq .

# 3. Or create a GitHub Pull Request / GitLab Merge Request for GitOps review
curl -s -X POST "http://localhost:8080/v1/approvals/<APPROVAL_ID>/create-pr" \
  -H "Content-Type: application/json" \
  -d '{"provider": "github"}' | jq .

# 4. Or reject the pending approval
curl -s -X POST "http://localhost:8080/v1/approvals/<APPROVAL_ID>/reject" \
  -H "Content-Type: application/json" \
  -d '{"approvedBy": "sre-admin"}' | jq .
```

---

### Method 2: Remediate via Chat Window & MCP Tools

In **VS Code GitHub Copilot Chat**, **Cursor**, **Claude**, or any connected MCP chat window, use natural language prompts. The agent invokes the corresponding `kube-doctor` MCP tools directly:

| What You Ask in Chat | MCP Tool Invoked by Agent | What Happens |
| :--- | :--- | :--- |
| *"Scan the cluster and fix any unhealthy pods or operator CRs."* | `scan_and_remediate_cluster_health` | Scans live cluster, patches within-quota workloads/CRDs, and stages approvals for quota/node issues. |
| *"What active incidents or pending approvals exist across our clusters?"* | `list_active_incidents` / `list_managed_clusters` | Returns active/resolved incidents, root-cause analyses, ownership chains, and `approvalId`s. |
| *"Show the full ownerReferences and CRD hierarchy for pod `noobaa-core-0` in `openshift-storage`."* | `inspect_resource_hierarchy` | Walks up to 8 hops (`Pod -> StatefulSet -> NooBaa -> StorageCluster` + sibling CRs) and reports status conditions. |
| *"Pod `redis-cluster-0` in `cache-prod` has a full PVC (`20Gi`). Diagnose and remediate it."* | `diagnose_and_remediate_cluster_issue` | Introspects the owning CRD OpenAPI v3 schema, selects the storage field via AI, and patches the CR to `40Gi`. |
| *"Approve remediation request `rem-req-...`."* | `approve_remediation_request` | Marks the `RemediationRequest` CRD approved and executes the `ResourceQuota` expansion + follow-up workload/CR patch. |
| *"Verify whether our recent remediations succeeded."* | `verify_remediation_effectiveness` | Checks live Pod/CR readiness, container restarts, and drift, returning a `0–100` Effectiveness Score. |

You can also remediate via the **OpenAI-compatible `/v1/chat/completions` API** from `curl` or any chat UI:
```bash
curl -s -X POST "http://localhost:8080/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "kube-doctor-1.35-ocp-4.22",
    "messages": [
      {
        "role": "user",
        "content": "Pod payments-db-0 in namespace prod-payments was OOMKilled (exit code 137) with memory limit 512Mi, quota limit 1Gi and used 800Mi. Diagnose and remediate."
      }
    ]
  }' | jq -r '.choices[0].message.content'
```

---

### Method 3: Remediate via the OpenShift Console UI Plugin

`kube-doctor` includes a built-in **OpenShift 4.22+ Console Plugin UI** served at:
- **Standalone / Port-Forward URL**: `http://localhost:8080/ui` (or `http://localhost:8080/console-plugin/`)
- **OpenShift Web Console**: Integrated via the `kube-doctor-console-plugin` `ConsolePlugin` resource ([`deploy/openshift/console-plugin.yaml`](./deploy/openshift/console-plugin.yaml)).

#### Step-by-Step Remediation in the UI:
1. **Scan & Inspect Incidents (`Active Incidents & Lineage` Tab)**:
   - Click **Scan Cluster Now** in the top header to trigger an immediate cluster-wide scan (`POST /v1/cluster/scan?force=true`).
   - View every active and resolved incident with its `Criticality` (`P0-CRITICAL` / `P1-HIGH`), `Severity` (`SEV-1` / `SEV-2`), affected namespace/resource, and AI root-cause summary.
2. **Approve, Reject, or Create GitOps PRs (`In-Cluster & GitOps Approvals` Tab)**:
   - Switch to the **In-Cluster & GitOps Approvals** tab to view all staged `RemediationRequest` CRDs and exact JSON Merge Patch payloads.
   - For any `PENDING` request, click:
     - **Approve & Execute In-Cluster** — immediately executes the `ResourceQuota` expansion, Operator CR patch, or `MachineSet` scale in the cluster.
     - **Create GitHub PR** or **Create GitLab MR** — opens a Pull/Merge Request against your configured GitOps repository. Merging the PR/MR automatically triggers execution via `/v1/webhooks/github` or `/v1/webhooks/gitlab`.
     - **Reject** — rejects and closes the `RemediationRequest`.
3. **Configure Governance & SCM OAuth (`GitHub & GitLab OAuth Integration` Tab)**:
   - Choose your cluster's **Default Approval Mode**:
     - `HYBRID` (In-Cluster `RemediationRequest` CRD + GitHub PR / GitLab MR)
     - `IN_CLUSTER` (Direct in-cluster approval inside the OpenShift Console)
     - `GITHUB_PR` (GitHub OAuth Pull Request required)
     - `GITLAB_MR` (GitLab OAuth Merge Request required)
   - Connect your **GitHub OAuth App** or **GitLab OAuth App** credentials and target GitOps repository (`owner/repo` and base branch).
