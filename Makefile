IMAGE_NAME   ?= kube-doctor
IMAGE_TAG    ?= v1.35-ocp4.22
IMAGE        ?= localhost/$(IMAGE_NAME):$(IMAGE_TAG)
IMAGE_TAR    ?= /tmp/kube-doctor-image.tar
KIND_CLUSTER ?= kind

.PHONY: all build test dataset train-qlora train-axolotl-podman ollama-build podman-build kind-load deploy-k8s deploy-openshift undeploy

all: test build

## Build local kube-doctor Go binary
build:
	CGO_ENABLED=0 go build -o bin/kube-doctor ./cmd/kube-doctor

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

## Build container image with Podman (uses multi-stage golang:1.23-alpine builder)
podman-build:
	podman build -t $(IMAGE) .

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

## Deploy RemediationRequest CRD + kube-doctor + kubernetes-mcp-server sidecar + Route to OpenShift 4.22+
deploy-openshift:
	oc apply -f deploy/crd/remediationrequest.kubedoctor.io.yaml
	oc apply -f deploy/openshift/deployment.yaml
	oc -n kube-doctor-system rollout status deployment/kube-doctor --timeout=120s

## Remove kube-doctor from the cluster
undeploy:
	kubectl delete -f deploy/kubernetes/deployment.yaml --ignore-not-found
	kubectl delete -f deploy/crd/remediationrequest.kubedoctor.io.yaml --ignore-not-found
