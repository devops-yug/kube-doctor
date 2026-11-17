# `cmd/kube-doctor` — System Architecture Overview

This document describes the architecture and component wiring of the `kube-doctor` binary ([`cmd/kube-doctor/main.go`](cmd/kube-doctor/main.go)).

---

## Architecture Diagram

```mermaid
flowchart TD
    subgraph Agents["External Agents, IDEs & UI"]
        Copilot["GitHub Copilot / VS Code / Cursor / Claude"]
        ConsoleUI["OpenShift 4.22+ Console Plugin (/ui)"]
        K8sMCPClientSide["Client-Side containers/kubernetes-mcp-server"]
    end

    subgraph KubeDoctor["kube-doctor Pod / Gateway (Go)"]
        OpenAIAPI["POST /v1/chat/completions\nPOST /copilot/agent"]
        MCPServer["POST /mcp (JSON-RPC 2.0)"]
        IngestAPI["POST /v1/diagnostics/ingest\nPOST /v1/cluster/scan"]
        ApprovalAPI["POST /v1/approvals/{id}/approve"]
        HubAPI["GET/POST /v1/hub/clusters"]

        Watcher["15s Autonomous Watcher & Alertmanager Webhook\n(internal/watcher)"]
        LLMEngine["Specialized LLM Orchestrator\n(kube-doctor-1.35-ocp-4.22)"]
        RAGEngine["K8s 1.35+ & OCP 4.22+ RAG Engine\n(internal/rag)"]
        TriageEngine["Criticality & Severity Remediation Engine\n(internal/remediation)"]
        AdapterEngine["Recursive 8-Hop Lineage & Live CRD OpenAPI AI Adapter\n(internal/k8s + internal/adapters)"]
        GitOpsMgr["GitOps PR & RemediationRequest CRD Manager\n(internal/gitops)"]
        K8sMCPBridge["K8sMCPClient Bridge\n(Streamable HTTP :8081/mcp or Stdio)"]
        K8sMCPSidecar["containers/kubernetes-mcp-server Sidecar\n(quay.io/containers/kubernetes_mcp_server)"]
    end

    subgraph Cluster["Kubernetes 1.35+ / OpenShift 4.22+ Cluster"]
        StsResize["Workload & Operator CR Patch\n+ K8s 1.35 pods/resize (AUTO_REMEDIATE)"]
        QuotaPatch["Namespace ResourceQuota Expansion\n(APPROVAL_REQUIRED)"]
        MachineScale["OpenShift 4.22 MachineSet Scale\n(APPROVAL_REQUIRED)"]
        RemReqCRD["RemediationRequest CRD\n(kubedoctor.io/v1alpha1)"]
    end

    Copilot <-->|"1. Prompt / 2. tool_calls"| OpenAIAPI
    Copilot <-->|"JSON-RPC 2.0"| MCPServer
    ConsoleUI <-->|"Scan / Approve / GitOps OAuth"| ApprovalAPI
    Copilot <--> K8sMCPClientSide
    OpenAIAPI --> LLMEngine
    MCPServer --> TriageEngine
    Watcher --> LLMEngine
    LLMEngine --> RAGEngine
    LLMEngine --> TriageEngine
    IngestAPI --> TriageEngine
    TriageEngine <--> AdapterEngine
    TriageEngine <--> K8sMCPBridge
    K8sMCPBridge <--> K8sMCPSidecar
    K8sMCPSidecar -->|"Within Quota (P1/SEV-2)"| StsResize
    TriageEngine -->|"Quota Exceeded or Machine Missing (P0/SEV-1)"| GitOpsMgr
    GitOpsMgr --> RemReqCRD
    ApprovalAPI --> GitOpsMgr
    GitOpsMgr -->|"Approved Execution"| K8sMCPBridge
    K8sMCPSidecar --> QuotaPatch
    K8sMCPSidecar --> MachineScale
```

---

## Internal Package Map

| Package | Path | Responsibility |
| :--- | :--- | :--- |
| `main` | [`cmd/kube-doctor/main.go`](cmd/kube-doctor/main.go) | Single-binary entrypoint supporting `--mode=standalone\|hub\|agent` |
| `api` | [`internal/api/`](internal/api/server.go) | HTTP gateway (`/v1/chat/completions`, `/copilot/agent`, `/mcp`, `/ui`, `/v1/cluster/scan`, `/v1/incidents`, `/v1/approvals`, `/v1/hub/*`, `/v1/gitops/*`) |
| `k8s` | [`internal/k8s/`](internal/k8s/client.go) | K8s 1.35+ / OCP 4.22+ REST client, live cluster scanner, 8-hop `ownerReferences` lineage walker, and live CRD OpenAPI v3 schema inspector |
| `adapters` | [`internal/adapters/`](internal/adapters/registry.go) | Universal AI + CRD OpenAPI v3 Schema Operator Adapter and optional YAML hints (`configs/adapters/`) |
| `remediation` | [`internal/remediation/`](internal/remediation/engine.go) | Criticality/Severity triage (`P0-CRITICAL`/`SEV-1` vs `P1-HIGH`/`SEV-2`) and `AUTO_REMEDIATE` vs `APPROVAL_REQUIRED` execution engine |
| `gitops` | [`internal/gitops/`](internal/gitops/manager.go) | `RemediationRequest` CRD (`kubedoctor.io/v1alpha1`) lifecycle, GitHub PR / GitLab MR creation, OAuth flows, and SCM webhooks |
| `watcher` | [`internal/watcher/`](internal/watcher/watcher.go) | 15-second autonomous reconciliation loop, Alertmanager v4 webhook receiver, and Post-Remediation Effectiveness Monitor |
| `hub` | [`internal/hub/`](internal/hub/manager.go) | Multi-Cluster Hub & Spoke Agent fleet manager, heartbeat ingestion, and per-cluster command queues |
| `operator` | [`internal/operator/`](internal/operator/controller.go) | OpenShift Operator reconciler for `KubeDoctor` and `RemediationRequest` CRs |
| `llm` | [`internal/llm/`](internal/llm/engine.go) | Specialized LLM orchestrator (`kube-doctor-1.35-ocp-4.22` via Ollama/vLLM + RAG + MCP tool-calling) |
| `rag` | [`internal/rag/`](internal/rag/engine.go) | Kubernetes `>= 1.35` & OpenShift `>= 4.22` runbook retrieval engine |
| `mcp` | [`internal/mcp/`](internal/mcp/README.md) | JSON-RPC 2.0 / Streamable HTTP MCP server (`/mcp`) and client bridge to `containers/kubernetes-mcp-server` |
