#!/usr/bin/env python3
"""
MCP Stdio-to-HTTP Bridge for kube-doctor.
Logs diagnostic trace to scripts/mcp_bridge_debug.log so we can inspect
what VS Code sends over stdin/stdout.
"""

import datetime
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

MCP_HTTP_URL = os.environ.get("KUBE_DOCTOR_MCP_URL", "http://127.0.0.1:8080/mcp")
LOG_FILE = Path(__file__).parent / "mcp_bridge_debug.log"

# Bypass any environment HTTP_PROXY / http_proxy so 127.0.0.1:8080 always hits kubectl port-forward directly
NO_PROXY_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def log_debug(msg: str):
    try:
        with LOG_FILE.open("a", encoding="utf-8") as f:
            ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def is_port_open(host: str = "127.0.0.1", port: int = 8080) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def ensure_port_forward(force_restart: bool = False) -> bool:
    if not force_restart and is_port_open():
        return True
    try:
        if force_restart:
            subprocess.run(
                ["pkill", "-f", "port-forward svc/kube-doctor 8080:8080"],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            time.sleep(0.3)
        log_debug("Auto-starting kubectl -n kube-doctor-system port-forward svc/kube-doctor 8080:8080")
        subprocess.Popen(
            ["kubectl", "-n", "kube-doctor-system", "port-forward", "svc/kube-doctor", "8080:8080"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        for _ in range(15):
            time.sleep(0.2)
            if is_port_open():
                log_debug("kubectl port-forward is listening on 127.0.0.1:8080")
                return True
    except Exception as exc:
        log_debug(f"ensure_port_forward error: {exc}")
    return is_port_open()


TOOLS_CATALOG = [
    {
        "name": "scan_and_remediate_cluster_health",
        "description": (
            "Autonomously scan the live Kubernetes/OpenShift cluster right now to discover any unhealthy Pods "
            "(OOMKilled, CrashLoopBackOff, ImagePullBackOff, Evicted), Nodes (NodeNotReady), PVCs (Pending), "
            "and Namespace ResourceQuotas directly from the Kubernetes API server, and automatically execute or stage remediation."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "forceReevaluate": {
                    "type": "boolean",
                    "description": "If true, re-evaluates currently unhealthy resources even if they were already processed in the last 5 minutes.",
                }
            },
        },
    },
    {
        "name": "list_active_incidents",
        "description": (
            "List all cluster incidents autonomously discovered by the background cluster watcher, Spoke Cluster Agents, "
            "or Prometheus Alertmanager webhooks, including their triage assessments, operator ownership chains, and pending approval IDs."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "clusterId": {
                    "type": "string",
                    "description": "Optional cluster ID filter when managing multiple clusters.",
                }
            },
        },
    },
    {
        "name": "list_managed_clusters",
        "description": (
            "List all Kubernetes >= 1.35 and OpenShift >= 4.22 clusters registered with the kube-doctor Multi-Cluster Hub, "
            "including their health status, active incident counts, and installed operator adapters."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "inspect_resource_hierarchy",
        "description": (
            "Walk the full multi-hop metadata.ownerReferences and Operator Custom Resource hierarchy (up to 8 levels deep, "
            "e.g. Pod -> StatefulSet -> NooBaa -> BackingStore/BucketClass -> StorageCluster, or Pod -> Cluster.postgresql.cnpg.io) "
            "and return the complete parent/child CRD topology and status conditions."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "apiVersion": {"type": "string"},
                "kind": {"type": "string"},
                "name": {"type": "string"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "list_operator_adapters",
        "description": (
            "List all registered Operator Custom Resource Adapters (built-in Go adapters like cloudnative-pg, noobaa-odf, strimzi-kafka "
            "and declarative YAML profiles like percona-pg) and their supported GVKs."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "verify_remediation_effectiveness",
        "description": (
            "Run the Post-Remediation Effectiveness Monitor across all remediated incidents to verify whether target "
            "Pods/CustomResources recovered (Ready=True), check for post-fix container restarts, detect spec drift, "
            "and compute Effectiveness Scores (0-100)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "diagnose_and_remediate_cluster_issue",
        "description": (
            "Diagnose a Kubernetes >= 1.35 / OpenShift >= 4.22 cluster event across all 26 IssueTypes "
            "(e.g. OOMKilled StatefulSet pod, Namespace ResourceQuota exceeded, missing OpenShift MachineSet node, "
            "PVC full, NodeNotReady, DRA GPU failure, PDB drain blocker) and execute or stage corrective action."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "podName": {"type": "string"},
                "ownerKind": {"type": "string"},
                "ownerName": {"type": "string"},
                "exitCode": {"type": "integer"},
                "reason": {"type": "string"},
                "message": {"type": "string"},
                "currentMemoryLimit": {"type": "string"},
                "currentMemoryRequest": {"type": "string"},
                "quotaName": {"type": "string"},
                "quotaMemoryLimit": {"type": "string"},
                "quotaMemoryUsed": {"type": "string"},
                "machineSetName": {"type": "string"},
                "expectedMachines": {"type": "integer"},
                "readyMachines": {"type": "integer"},
            },
            "required": ["reason"],
        },
    },
    {
        "name": "approve_remediation_request",
        "description": (
            "Approve a pending RemediationRequest CRD and GitOps Pull Request "
            "(e.g. for Namespace ResourceQuota expansion or OpenShift 4.22 MachineSet autoscaling) "
            "and execute the cluster mutation."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "approvalId": {"type": "string"},
                "approvedBy": {"type": "string"},
            },
            "required": ["approvalId"],
        },
    },
    {
        "name": "query_k8s_135_ocp_422_knowledge",
        "description": "Query the specialized Kubernetes >= 1.35 and OpenShift >= 4.22 RAG knowledge base.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "events_list",
        "description": "List Kubernetes events (warnings, errors, state changes) and active kube-doctor incident summaries via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "fieldSelector": {"type": "string"},
            },
        },
    },
    {
        "name": "namespaces_list",
        "description": "List all Kubernetes Namespaces via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "pods_list",
        "description": "List all Kubernetes Pods across all namespaces (or filtered by labelSelector/fieldSelector) via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "labelSelector": {"type": "string"},
                "fieldSelector": {"type": "string"},
            },
        },
    },
    {
        "name": "pods_list_in_namespace",
        "description": "List all Kubernetes Pods in a specific namespace via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "namespace": {"type": "string"},
                "labelSelector": {"type": "string"},
                "fieldSelector": {"type": "string"},
            },
            "required": ["namespace"],
        },
    },
    {
        "name": "pods_get",
        "description": "Get a Kubernetes Pod in the current or provided namespace via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "namespace": {"type": "string"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "pods_log",
        "description": "Get logs of a Kubernetes Pod (including previous terminated container logs) via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "namespace": {"type": "string"},
                "container": {"type": "string"},
                "previous": {"type": "boolean"},
                "tail": {"type": "integer"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "pods_top",
        "description": "List CPU and memory consumption for Kubernetes Pods via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "namespace": {"type": "string"},
                "all_namespaces": {"type": "boolean"},
                "label_selector": {"type": "string"},
            },
        },
    },
    {
        "name": "nodes_stats_summary",
        "description": "Get node resource usage and Kubernetes 1.35+ cgroup v2 PSI metrics via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
            },
            "required": ["name"],
        },
    },
    {
        "name": "resources_list",
        "description": "List any Kubernetes or OpenShift resources by apiVersion and kind (e.g. Pod, StatefulSet, Deployment, Node, PersistentVolumeClaim, ResourceQuota) via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "apiVersion": {"type": "string"},
                "kind": {"type": "string"},
                "namespace": {"type": "string"},
                "labelSelector": {"type": "string"},
                "fieldSelector": {"type": "string"},
            },
            "required": ["apiVersion", "kind"],
        },
    },
    {
        "name": "resources_get",
        "description": "Get any Kubernetes or OpenShift resource (StatefulSet, ResourceQuota, MachineSet, etc.) via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "apiVersion": {"type": "string"},
                "kind": {"type": "string"},
                "name": {"type": "string"},
                "namespace": {"type": "string"},
            },
            "required": ["apiVersion", "kind", "name"],
        },
    },
    {
        "name": "resources_create_or_update",
        "description": "Create or update any Kubernetes/OpenShift resource via Server-Side Apply using containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "resource": {"type": "string"},
            },
            "required": ["resource"],
        },
    },
    {
        "name": "resources_scale",
        "description": "Get or update the scale of a StatefulSet, Deployment, or OpenShift MachineSet via containers/kubernetes-mcp-server",
        "inputSchema": {
            "type": "object",
            "properties": {
                "apiVersion": {"type": "string"},
                "kind": {"type": "string"},
                "name": {"type": "string"},
                "namespace": {"type": "string"},
                "scale": {"type": "integer"},
            },
            "required": ["apiVersion", "kind", "name"],
        },
    },
]


def handle_rpc(line: str) -> str | None:
    line = line.strip()
    if not line:
        return None

    log_debug(f"RECV: {line}")

    try:
        msg = json.loads(line)
    except json.JSONDecodeError as exc:
        log_debug(f"JSON decode error: {exc}")
        return None

    method = msg.get("method", "")
    msg_id = msg.get("id")
    params = msg.get("params") or {}

    if msg_id is None or method.startswith("notifications/"):
        log_debug(f"ACK notification: {method}")
        return None

    if method == "initialize":
        protocol_version = params.get("protocolVersion") or "2024-11-05"
        resp = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": protocol_version,
                "serverInfo": {
                    "name": "kube-doctor-mcp",
                    "version": "1.35.0-ocp4.22",
                },
                "capabilities": {
                    "tools": {"listChanged": False},
                },
            },
        }
        out = json.dumps(resp, separators=(",", ":"))
        log_debug(f"SEND: {out}")
        return out

    if method == "server/discover":
        meta = params.get("_meta") or {}
        protocol_version = meta.get("io.modelcontextprotocol/protocolVersion") or "2026-07-28"
        resp = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": protocol_version,
                "serverInfo": {
                    "name": "kube-doctor-mcp",
                    "version": "1.35.0-ocp4.22",
                },
                "capabilities": {
                    "tools": {"listChanged": False},
                },
                "tools": TOOLS_CATALOG,
            },
        }
        out = json.dumps(resp, separators=(",", ":"))
        log_debug(f"SEND server/discover ({len(TOOLS_CATALOG)} tools)")
        return out

    if method == "tools/list":
        resp = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {"tools": TOOLS_CATALOG},
        }
        out = json.dumps(resp, separators=(",", ":"))
        log_debug(f"SEND tools/list ({len(TOOLS_CATALOG)} tools)")
        return out

    if method in ("ping", "logging/setLevel"):
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {}}, separators=(",", ":"))
    if method == "prompts/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"prompts": []}}, separators=(",", ":"))
    if method == "resources/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"resources": []}}, separators=(",", ":"))
    if method == "resources/templates/list":
        return json.dumps({"jsonrpc": "2.0", "id": msg_id, "result": {"resourceTemplates": []}}, separators=(",", ":"))

    ensure_port_forward(force_restart=False)

    last_exc = None
    for attempt in range(2):
        req = urllib.request.Request(
            MCP_HTTP_URL,
            data=line.encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        try:
            with NO_PROXY_OPENER.open(req, timeout=30) as resp:
                body = resp.read().decode("utf-8").strip()
                if not body:
                    return json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": msg_id,
                            "result": {"content": [{"type": "text", "text": "OK"}]},
                        },
                        separators=(",", ":"),
                    )
                parsed = json.loads(body)
                out = json.dumps(parsed, separators=(",", ":"))
                log_debug(f"SEND tool result: {out[:200]}")
                return out
        except Exception as exc:
            last_exc = exc
            log_debug(f"HTTP forward attempt {attempt + 1} error: {exc}")
            if attempt == 0:
                ensure_port_forward(force_restart=True)

    return json.dumps(
        {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "isError": True,
                "content": [
                    {
                        "type": "text",
                        "text": (
                            f"Failed to reach kube-doctor pod at {MCP_HTTP_URL}: {last_exc}\n"
                            "Make sure `kubectl -n kube-doctor-system port-forward svc/kube-doctor 8080:8080` is running."
                        ),
                    }
                ],
            },
        },
        separators=(",", ":"),
    )


def main():
    log_debug(f"=== Bridge started (PID={os.getpid()}, argv={sys.argv}) ===")
    for raw_line in sys.stdin:
        out = handle_rpc(raw_line)
        if out:
            sys.stdout.write(out + "\n")
            sys.stdout.flush()
    log_debug(f"=== Bridge stdin closed (PID={os.getpid()}) ===")


if __name__ == "__main__":
    main()
