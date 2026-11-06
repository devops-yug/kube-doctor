#!/usr/bin/env python3
"""
Interactive CLI & Scenario Playground for the specialized `kube-doctor-1.35-ocp-4.22` LLM.
Talks directly to:
  1. Your running Ollama instance (`http://127.0.0.1:11434/v1/chat/completions`, model `kube-doctor-1.35-ocp-4.22`), OR
  2. The `kube-doctor` gateway (`http://127.0.0.1:8080/v1/chat/completions`), which enriches your prompt with
     K8s 1.35+ / OCP 4.22+ RAG runbooks, Operator Adapter triage (`Cluster.postgresql.cnpg.io`, `NooBaa` 5-CR hierarchy),
     and forwards to Ollama (`http://127.0.0.1:11434`).

Usage:
  1. Run all built-in showcase scenarios against your Ollama LLM:
     python3 scripts/llm_playground.py --scenarios

  2. Send a single prompt:
     python3 scripts/llm_playground.py "Pod pg-cluster-1 in Cluster.postgresql.cnpg.io is OOMKilled with exit code 137"

  3. Start an interactive REPL session with the LLM:
     python3 scripts/llm_playground.py
"""

import json
import os
import socket
import subprocess
import sys
import time
import urllib.request

OLLAMA_URL = os.environ.get("OLLAMA_BASE_URL", "http://127.0.0.1:11434")
GATEWAY_URL = os.environ.get("KUBE_DOCTOR_URL", "http://127.0.0.1:8080")
MODEL_NAME = os.environ.get("KUBE_DOCTOR_MODEL", "kube-doctor-1.35-ocp-4.22")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def is_port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            return True
    except OSError:
        return False


def detect_ollama_model() -> str:
    """Detect if kube-doctor-1.35-ocp-4.22 is loaded in Ollama, or pick the available model."""
    try:
        req = urllib.request.Request(f"{OLLAMA_URL}/api/tags", method="GET")
        with OPENER.open(req, timeout=3) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            models = [m.get("name", "") for m in data.get("models", [])]
            for m in models:
                if "kube-doctor" in m:
                    return m
            if models:
                return models[0]
    except Exception:
        pass
    return MODEL_NAME


def ensure_gateway_running() -> subprocess.Popen | None:
    if is_port_open("127.0.0.1", 8080):
        return None
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    bin_path = os.path.join(root_dir, "bin", "kube-doctor")
    if not os.path.exists(bin_path):
        subprocess.run(["go", "build", "-o", bin_path, "./cmd/kube-doctor"], cwd=root_dir, check=True)
    env = os.environ.copy()
    env.setdefault("UPSTREAM_LLM_BASE_URL", OLLAMA_URL)
    proc = subprocess.Popen(
        [bin_path],
        cwd=root_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    for _ in range(25):
        time.sleep(0.1)
        if is_port_open("127.0.0.1", 8080):
            return proc
    return proc


def chat_with_llm(prompt: str, direct_ollama: bool = False, tools: list | None = None) -> dict:
    model = detect_ollama_model()
    target_base = OLLAMA_URL if direct_ollama else GATEWAY_URL
    payload = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "stream": False,
    }
    if tools:
        payload["tools"] = tools

    req = urllib.request.Request(
        f"{target_base}/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with OPENER.open(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def run_showcase_scenarios(direct_ollama: bool = False):
    model = detect_ollama_model()
    print(f"Using Ollama model: {model} (Ollama={OLLAMA_URL}, Gateway={GATEWAY_URL})")
    scenarios = [
        (
            "Scenario 1: Within-Quota StatefulSet Pod OOMKilled (ExitCode 137) -> AUTO_REMEDIATE via K8s 1.35 pods/resize",
            "Pod postgres-ha-0 in StatefulSet postgres-ha terminated with OOMKilled (ExitCode 137) at 1Gi limit.",
        ),
        (
            "Scenario 2: Namespace ResourceQuota Exceeded on OOMKilled -> APPROVAL_REQUIRED (RemediationRequest CRD + GitOps PR)",
            "StatefulSet postgres-ha pod postgres-ha-0 is OOMKilled (ExitCode 137), and bumping memory exceeds Namespace ResourceQuota production-compute-quota (1800Mi/2Gi).",
        ),
        (
            "Scenario 3: CloudNativePG Custom Resource (Cluster.postgresql.cnpg.io) OOMKilled -> Operator Adapter patches CR",
            "Pod pg-cluster-1 managed by CloudNativePG Cluster.postgresql.cnpg.io/pg-cluster is crashing with OOMKilled (exit code 137).",
        ),
        (
            "Scenario 4: NooBaa / OpenShift Data Foundation 5-Custom-Resource Hierarchy Degraded -> Multi-Hop Lineage Resolution",
            "NooBaa pod noobaa-core-0 is failing because BackingStore noobaa-pv-backing-store under StorageCluster is in Rejected phase.",
        ),
        (
            "Scenario 5: OpenShift 4.22 Missing Worker Machine in MachineSet -> APPROVAL_REQUIRED to Autoscale MachineSet",
            "OpenShift 4.22 MachineSet ocp422-worker-us-east-1a has 2/3 ready machines (MachineMissing alert).",
        ),
    ]

    for title, prompt in scenarios:
        print("\n" + "=" * 95)
        print(f"SCENARIO: {title}")
        print(f"PROMPT:   {prompt}")
        print("-" * 95)
        resp = chat_with_llm(prompt, direct_ollama=direct_ollama)
        if resp.get("x_kube_doctor_triage"):
            triage = resp["x_kube_doctor_triage"]
            print(
                f"[Gateway Triage] Issue={triage.get('issueType')} | "
                f"Criticality={triage.get('criticality')} | Severity={triage.get('severity')} | "
                f"Adapter={triage.get('operatorAdapter') or 'core-k8s'}"
            )
            print("-" * 95)
        msg = resp["choices"][0]["message"]
        print(msg.get("content", ""))


def interactive_repl(direct_ollama: bool = False):
    model = detect_ollama_model()
    print("=" * 85)
    print(f"  kube-doctor Interactive LLM Playground (Model: {model})")
    print(f"  Ollama Backend:  {OLLAMA_URL}")
    print(f"  Gateway Pipeline: {GATEWAY_URL}/v1/chat/completions (RAG + Operator Adapters + Ollama)")
    print("  Type any cluster scenario or question (or 'exit' to quit)")
    print("=" * 85)
    while True:
        try:
            prompt = input("\n[You] > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break
        if not prompt or prompt.lower() in ("exit", "quit"):
            break
        resp = chat_with_llm(prompt, direct_ollama=direct_ollama)
        if resp.get("x_kube_doctor_triage"):
            triage = resp["x_kube_doctor_triage"]
            print(
                f"\n[Triage Metadata] Issue={triage.get('issueType')} | "
                f"Criticality={triage.get('criticality')} | Severity={triage.get('severity')} | "
                f"Adapter={triage.get('operatorAdapter') or 'core-k8s'}"
            )
        msg = resp["choices"][0]["message"]
        print(f"\n[{model}]:\n")
        print(msg.get("content", ""))


def main():
    direct_ollama = "--direct-ollama" in sys.argv
    args = [a for a in sys.argv[1:] if a != "--direct-ollama"]

    proc = None
    if not direct_ollama:
        proc = ensure_gateway_running()
    try:
        if args and args[0] == "--scenarios":
            run_showcase_scenarios(direct_ollama=direct_ollama)
        elif args:
            prompt = " ".join(args)
            resp = chat_with_llm(prompt, direct_ollama=direct_ollama)
            print(resp["choices"][0]["message"].get("content", ""))
        else:
            interactive_repl(direct_ollama=direct_ollama)
    finally:
        if proc is not None:
            proc.terminate()


if __name__ == "__main__":
    main()
