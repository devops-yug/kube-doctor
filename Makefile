IMAGE_NAME   ?= kube-doctor
IMAGE_TAG    ?= v1.35-ocp4.22
IMAGE        ?= localhost/$(IMAGE_NAME):$(IMAGE_TAG)
IMAGE_TAR    ?= /tmp/kube-doctor-image.tar
KIND_CLUSTER ?= kind
TARGET_ARCH  ?= $(shell go env GOARCH)
HUB_IP       ?= $(shell hostname -I 2>/dev/null | awk '{print $$1}')
CLUSTER_ID   ?= raspberrypi-arm64

.PHONY: all build build-arm64 test dataset train-qlora train-axolotl-podman ollama-build podman-build podman-build-arm64 podman-build-multiarch kind-load deploy-k8s deploy-hub deploy-agent run-hub deploy-openshift undeploy

all: test build

## Build local kube-doctor Go binary (auto-detects host arch, or override with TARGET_ARCH=arm64)
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=$(TARGET_ARCH) go build -ldflags="-s -w" -o bin/kube-doctor ./cmd/kube-doctor

## Cross-compile kube-doctor Go binary for ARM64 (Raspberry Pi 4/5 64-bit OS)
build-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags="-s -w" -o bin/kube-doctor-linux-arm64 ./cmd/kube-doctor

## Run all unit & integration tests
test:
	go test -v ./...

## Generate the SFT + Tool-Calling dataset for QLoRA fine-tuning (all 26 IssueTypes)
dataset:
	python3 training/dataset_generator.py

## Train QLoRA adapter using standalone Python PEFT + TRL script
train-qlora: dataset
	python3 training/train_qlora.py

## Train QLoRA adapter using official Axolotl AI podman container (requires NVIDIA GPU)
train-axolotl-podman: dataset
	podman run --gpus all --rm -v $(PWD):/workspace winglian/axolotl:main-latest \
		accelerate launch -m axolotl.cli.train /workspace/training/qlora_config.yaml

## Build specialized model directly with Ollama (no GPU training required)
ollama-build:
	ollama create kube-doctor-1.35-ocp-4.22 -f training/Modelfile

## Build container image with Podman for current host arch (or override: make podman-build TARGET_ARCH=arm64)
podman-build:
	podman build --platform linux/$(TARGET_ARCH) --build-arg TARGETARCH=$(TARGET_ARCH) -t $(IMAGE) .

## Build ARM64 container image for Raspberry Pi (linux/arm64) and export to $(IMAGE_TAR)
podman-build-arm64:
	podman build --platform linux/arm64 --build-arg TARGETARCH=arm64 -t $(IMAGE) .
	podman save --format oci-archive -o $(IMAGE_TAR) $(IMAGE)
	@echo "Saved ARM64 image to $(IMAGE_TAR)"

## Build multi-arch manifest (linux/amd64 + linux/arm64) with Podman
podman-build-multiarch:
	-podman manifest rm $(IMAGE)-multiarch 2>/dev/null || true
	podman manifest create $(IMAGE)-multiarch
	podman build --platform linux/amd64,linux/arm64 --manifest $(IMAGE)-multiarch .

## Create a local kind cluster if it does not already exist
kind-cluster:
	@if ! kind get clusters 2>/dev/null | grep -qx "$(KIND_CLUSTER)"; then \
		echo "Creating kind cluster '$(KIND_CLUSTER)'..."; \
		kind create cluster --image kindest/node:v1.36.4 --name $(KIND_CLUSTER); \
	else \
		echo "Kind cluster '$(KIND_CLUSTER)' is already running."; \
	fi

## Load built Podman image directly into the kind node's containerd (k8s.io namespace)
## Bypasses kind CLI's containerd v2 (config version 4) check on Kubernetes 1.36+ nodes
kind-load: podman-build kind-cluster
	@for node in $$(kind get nodes --name $(KIND_CLUSTER)); do \
		echo "Loading $(IMAGE) into kind node $$node..."; \
		podman save --format docker-archive $(IMAGE) | podman exec -i $$node ctr --namespace=k8s.io images import -; \
	done

## Deploy RemediationRequest CRD + kube-doctor + kubernetes-mcp-server sidecar to Kubernetes 1.35+
deploy-k8s:
	kubectl apply -f deploy/crd/remediationrequest.kubedoctor.io.yaml
	kubectl apply -f deploy/kubernetes/deployment.yaml
	kubectl -n kube-doctor-system rollout restart deployment/kube-doctor
	kubectl -n kube-doctor-system rollout status deployment/kube-doctor --timeout=120s

## Deploy kube-doctor in Central Hub Mode (--mode=hub) on this machine's cluster
deploy-hub:
	kubectl apply -f deploy/crd/remediationrequest.kubedoctor.io.yaml
	kubectl apply -f deploy/kubernetes/hub-deployment.yaml
	kubectl -n kube-doctor-system rollout restart deployment/kube-doctor
	kubectl -n kube-doctor-system rollout status deployment/kube-doctor --timeout=120s
	@echo "Hub deployed! Expose to LAN for Raspberry Pi agent with:"
	@echo "  kubectl -n kube-doctor-system port-forward --address 0.0.0.0 svc/kube-doctor 8080:8080"

## Run kube-doctor directly on this host in Central Hub Mode (--mode=hub, listening on 0.0.0.0:8080)
run-hub: build
	KUBE_DOCTOR_MODE=hub KUBE_DOCTOR_CLUSTER_ID=hub-control-plane PORT=8080 ./bin/kube-doctor --mode=hub

## Forward Hub port 8080 on 0.0.0.0 inside WSL2/Linux and print Windows WSL2 LAN firewall commands
expose-hub:
	-pkill -f "port-forward.*svc/kube-doctor" 2>/dev/null || true
	kubectl -n kube-doctor-system port-forward --address 0.0.0.0 svc/kube-doctor 8080:8080 &
	@sleep 1
	@echo "Listening on 0.0.0.0:8080 inside WSL2."
	@echo "If running on WSL2, run this in an Administrator PowerShell on Windows so the Raspberry Pi can reach 192.168.68.59:8080:"
	@echo "  \$$wslIp = (wsl hostname -I).Trim().Split(' ')[0]; netsh interface portproxy add v4tov4 listenport=8080 listenaddress=0.0.0.0 connectport=8080 connectaddress=\$$wslIp; New-NetFirewallRule -DisplayName 'kube-doctor-hub-8080' -Direction Inbound -LocalPort 8080,11434 -Protocol TCP -Action Allow"

## Deploy kube-doctor in Spoke Agent Mode (--mode=agent) on Raspberry Pi (connects via 127.0.0.1:18080 SSH reverse tunnel by default)
deploy-agent:
	kubectl apply -f deploy/crd/remediationrequest.kubedoctor.io.yaml
	kubectl apply -f deploy/kubernetes/agent-deployment.yaml
	kubectl -n kube-doctor-system rollout restart deployment/kube-doctor
	kubectl -n kube-doctor-system rollout status deployment/kube-doctor --timeout=120s

## Deploy RemediationRequest CRD + kube-doctor + kubernetes-mcp-server sidecar + Route to OpenShift 4.22+
deploy-openshift:
	oc apply -f deploy/crd/remediationrequest.kubedoctor.io.yaml
	oc apply -f deploy/openshift/deployment.yaml
	oc -n kube-doctor-system rollout status deployment/kube-doctor --timeout=120s

## Remove kube-doctor from the cluster
undeploy:
	kubectl delete -f deploy/kubernetes/deployment.yaml --ignore-not-found
	kubectl delete -f deploy/crd/remediationrequest.kubedoctor.io.yaml --ignore-not-found
