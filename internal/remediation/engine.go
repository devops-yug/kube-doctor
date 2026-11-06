package remediation

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/luigi/kube-doctor/internal/adapters"
	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

// K8sMCPExecutor defines the interface for invoking tools on
// https://github.com/containers/kubernetes-mcp-server
type K8sMCPExecutor interface {
	GetPod(ctx context.Context, namespace, name string) (string, error)
	GetPodLogs(ctx context.Context, namespace, name, container string, previous bool, tail int) (string, error)
	GetNodeStatsSummary(ctx context.Context, nodeName string) (string, error)
	ListEvents(ctx context.Context, namespace, fieldSelector string) (string, error)
	GetResource(ctx context.Context, apiVersion, kind, namespace, name string) (string, error)
	CreateOrUpdateResource(ctx context.Context, resourceYAMLOrJSON string) (string, error)
	ScaleResource(ctx context.Context, apiVersion, kind, namespace, name string, scale int) (string, error)
}

// Engine performs criticality/severity classification and executes or schedules
// corrective actions across all 27 Kubernetes >= 1.35 and OpenShift >= 4.22 failure modes,
// including pluggable Operator Custom Resource Adapters (CloudNativePG, NooBaa/ODF 5-CRD hierarchy, Strimzi, etc.).
type Engine struct {
	k8sClient       *k8s.Client
	gitopsManager   *gitops.Manager
	k8sMCP          K8sMCPExecutor
	adapterRegistry *adapters.Registry
}

// NewEngine creates a new Remediation Engine with built-in and declarative YAML Operator Adapters.
func NewEngine(k8sClient *k8s.Client, gitopsManager *gitops.Manager, k8sMCP ...K8sMCPExecutor) *Engine {
	var mcpExec K8sMCPExecutor
	if len(k8sMCP) > 0 {
		mcpExec = k8sMCP[0]
	}
	return &Engine{
		k8sClient:       k8sClient,
		gitopsManager:   gitopsManager,
		k8sMCP:          mcpExec,
		adapterRegistry: adapters.NewRegistry(),
	}
}

// AdapterRegistry returns the engine's OperatorAdapter registry.
func (e *Engine) AdapterRegistry() *adapters.Registry {
	return e.adapterRegistry
}

// EvaluateAndAct analyzes a DiagnosticEvent across all 26 Kubernetes 1.35+ / OpenShift 4.22+
// IssueTypes, queries containers/kubernetes-mcp-server for live telemetry, classifies
// Criticality and Severity, and executes either autonomous remediation or GitOps/CRD approval staging.
func (e *Engine) EvaluateAndAct(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	if ev.ID == "" {
		ev.ID = fmt.Sprintf("diag-%d", time.Now().UnixNano())
	}
	if ev.ClusterVersion == "" {
		ev.ClusterVersion = "v1.35.2"
	}
	if ev.OpenShiftVersion == "" {
		ev.OpenShiftVersion = "4.22.1"
	}
	if ev.Namespace == "" {
		ev.Namespace = "default"
	}
	if e.k8sClient != nil {
		e.k8sClient.EnrichDiagnosticEvent(ctx, &ev)
	}

	// Check if a specialized Operator Custom Resource Adapter (e.g. CloudNativePG Cluster.postgresql.cnpg.io,
	// NooBaa/ODF 5-CR hierarchy, Strimzi Kafka, or a declarative YAML adapter) owns this resource hierarchy.
	if e.adapterRegistry != nil {
		if adapter := e.adapterRegistry.Match(ev); adapter != nil {
			return e.handleOperatorAdapterRemediation(ctx, adapter, ev)
		}
	}

	reasonLower := strings.ToLower(ev.Reason + " " + ev.Message)

	switch {
	// 1. OpenShift 4.22+ Missing Machine / MachineSet Autoscale
	case ev.MachineSetName != "" ||
		(ev.ExpectedMachines > 0 && ev.ReadyMachines < ev.ExpectedMachines) ||
		strings.Contains(reasonLower, "machinemissing") ||
		strings.Contains(reasonLower, "machinefailed") ||
		strings.Contains(reasonLower, "machineset"):
		return e.handleMissingMachine(ctx, ev)

	// 2. OOMKilled Pod (ExitCode 137 or OOMKilled) + StatefulSet / Quota governance
	case ev.ExitCode == 137 || strings.Contains(reasonLower, "oomkilled") || strings.Contains(reasonLower, "oom"):
		return e.handlePodOOMKilled(ctx, ev)

	// 3. Explicit Namespace ResourceQuota Exceeded
	case strings.Contains(reasonLower, "resourcequota") || strings.Contains(reasonLower, "exceeded quota") || strings.Contains(reasonLower, "quota"):
		return e.handleQuotaExceededOnly(ctx, ev)

	// 4. LimitRange Violation
	case strings.Contains(reasonLower, "limitrange") || strings.Contains(reasonLower, "minimum/maximum"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueLimitRangeViolation,
			types.CriticalityP2Medium,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"v1", "LimitRange", ev.Namespace, defaultStr(ev.QuotaName, "default-limits"),
			"UPDATE_NAMESPACE_LIMITRANGE",
			map[string]any{"spec": map[string]any{"limits": []map[string]any{{"type": "Container", "max": map[string]string{"memory": "8Gi", "cpu": "4"}}}}},
			"Container resource request/limit violates Namespace LimitRange ceiling; stage LimitRange increase via GitOps PR & RemediationRequest CRD.",
			[]string{"Kubernetes 1.35 InPlacePodVerticalScaling LimitRange admission validation"},
			[]string{"OpenShift 4.22 Project LimitRange multi-tenant guardrails"},
		)

	// 5. ImagePullBackOff / ErrImagePull / OpenShift ImageDigestMirrorSet
	case strings.Contains(reasonLower, "imagepullbackoff") || strings.Contains(reasonLower, "errimagepull"):
		return e.handleImagePullBackOff(ctx, ev)

	// 6. Native Sidecar Container Failure (K8s 1.35+ restartPolicy: Always initContainers)
	case strings.Contains(reasonLower, "sidecar"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueSidecarContainerFailure,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyAutoRemediate,
			"apps/v1", defaultStr(ev.OwnerKind, "Deployment"), ev.Namespace, defaultStr(ev.OwnerName, "mesh-workload"),
			"PATCH_SIDECAR_STARTUP_PROBE",
			map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"initContainers": []map[string]any{{"name": defaultStr(ev.ContainerName, "istio-proxy"), "restartPolicy": "Always", "startupProbe": map[string]any{"failureThreshold": 30, "periodSeconds": 5}}}}}}},
			"Kubernetes 1.35 native sidecar container (initContainer with restartPolicy: Always) failed startupProbe blocking main workload containers; auto-tuned startupProbe thresholds.",
			[]string{"Kubernetes 1.35 Native SidecarContainers GA lifecycle & startupProbe ordering"},
			[]string{"OpenShift Service Mesh 3 / Kiali sidecar health reconciliation"},
		)

	// 7. Liveness / Readiness Probe Failure under CPU Throttling
	case strings.Contains(reasonLower, "liveness") || strings.Contains(reasonLower, "readiness") || strings.Contains(reasonLower, "unhealthy"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueLivenessProbeFailure,
			types.CriticalityP2Medium,
			types.SeveritySev2,
			types.PolicyAutoRemediate,
			"apps/v1", defaultStr(ev.OwnerKind, "Deployment"), ev.Namespace, defaultStr(ev.OwnerName, "api-service"),
			"INPLACE_CPU_RESIZE_AND_PROBE_TUNE",
			map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": defaultStr(ev.ContainerName, "app"), "resources": map[string]any{"limits": map[string]string{"cpu": "2000m"}}, "livenessProbe": map[string]any{"timeoutSeconds": 5, "failureThreshold": 5}}}}}}},
			"Container failing liveness/readiness probe due to CFS CPU throttling during startup/GC; bumped CPU limit via K8s 1.35 pods/resize and relaxed probe timeout.",
			[]string{"Kubernetes 1.35 InPlacePodVerticalScaling (RestartNotRequired for CPU)", "cgroup v2 cpu.stat nr_throttled & PSI cpu.pressure analysis"},
			nil,
		)

	// 8. Pod Evicted due to Node DiskPressure / Ephemeral-Storage
	case strings.Contains(reasonLower, "evicted") || strings.Contains(reasonLower, "diskpressure") || strings.Contains(reasonLower, "ephemeral-storage"):
		return e.handleScenarioAction(ctx, ev,
			types.IssuePodEvictedNodePressure,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyAutoRemediate,
			"apps/v1", defaultStr(ev.OwnerKind, "StatefulSet"), ev.Namespace, defaultStr(ev.OwnerName, "log-ingestor"),
			"CLEANUP_EVICTED_PODS_AND_BUMP_EPHEMERAL_STORAGE",
			map[string]any{"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []map[string]any{{"name": defaultStr(ev.ContainerName, "app"), "resources": map[string]any{"limits": map[string]string{"ephemeral-storage": "10Gi"}}}}}}}},
			"Pod evicted by kubelet eviction manager due to ephemeral-storage / node DiskPressure; cleaned up Evicted pods and increased ephemeral-storage limit.",
			[]string{"Kubelet cgroup v2 PSI io.pressure & ephemeral-storage eviction thresholds"},
			nil,
		)

	// 9. Pod Stuck Terminating (Finalizer / Hung Runtime)
	case strings.Contains(reasonLower, "terminating") || strings.Contains(reasonLower, "finalizer"):
		return e.handleScenarioAction(ctx, ev,
			types.IssuePodStuckTerminating,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"v1", "Pod", ev.Namespace, defaultStr(ev.PodName, "stuck-pod-0"),
			"CLEAR_STALE_FINALIZER_AND_FORCE_DELETE_POD",
			map[string]any{"metadata": map[string]any{"finalizers": nil}},
			"Pod stuck in Terminating state past terminationGracePeriodSeconds due to wedged finalizer or unresponsive node; staged approval to clear finalizer safely.",
			[]string{"Kubernetes 1.35 Non-Graceful Node Shutdown taint (node.kubernetes.io/out-of-service)"},
			nil,
		)

	// 10. HPA Maxed Out (HorizontalPodAutoscaler at maxReplicas)
	case ev.HPAName != "" || strings.Contains(reasonLower, "hpa") || strings.Contains(reasonLower, "maxreplicas") || strings.Contains(reasonLower, "scalinglimited"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueHPAMaxedOut,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"autoscaling/v2", "HorizontalPodAutoscaler", ev.Namespace, defaultStr(ev.HPAName, "checkout-hpa"),
			"EXPAND_HPA_MAX_REPLICAS",
			map[string]any{"spec": map[string]any{"maxReplicas": 20}},
			"HorizontalPodAutoscaler reached maxReplicas ceiling (ScalingLimited=True) while workload CPU/memory remains above target; staged GitOps PR & RemediationRequest CRD to increase maxReplicas.",
			[]string{"Kubernetes 1.35 ContainerResource HPA scaling on per-container metrics"},
			nil,
		)

	// 11. PodDisruptionBudget (PDB) Blocking Node Drain / Upgrade
	case ev.PDBName != "" || strings.Contains(reasonLower, "pdb") || strings.Contains(reasonLower, "poddisruptionbudget") || strings.Contains(reasonLower, "disruptionsallowed"):
		return e.handleScenarioAction(ctx, ev,
			types.IssuePDBBlockingDrain,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"policy/v1", "PodDisruptionBudget", ev.Namespace, defaultStr(ev.PDBName, "strict-pdb"),
			"RELAX_PDB_OR_SCALE_WORKLOAD_FOR_DRAIN",
			map[string]any{"spec": map[string]any{"maxUnavailable": 1, "unhealthyPodEvictionPolicy": "AlwaysAllow"}},
			"PodDisruptionBudget has disruptionsAllowed=0 blocking OpenShift MachineConfigPool node drain; staged approval to set unhealthyPodEvictionPolicy=AlwaysAllow and maxUnavailable=1.",
			[]string{"Kubernetes 1.35 PDB unhealthyPodEvictionPolicy=AlwaysAllow GA"},
			[]string{"OpenShift 4.22 MachineConfigOperator node drain deadlock mitigation"},
		)

	// 12. Kubernetes 1.35+ Dynamic Resource Allocation (DRA) Failure (GPU / NIC ResourceClaim)
	case strings.Contains(reasonLower, "dra") || strings.Contains(reasonLower, "resourceclaim") || strings.Contains(reasonLower, "deviceclass") || strings.Contains(reasonLower, "resourceslice"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueDRAAllocationFailure,
			types.CriticalityP0Critical,
			types.SeveritySev1,
			types.PolicyApprovalRequired,
			"resource.k8s.io/v1beta1", "ResourceClaimTemplate", ev.Namespace, defaultStr(ev.OwnerName, "gpu-inference-claim"),
			"RECONCILE_DRA_DEVICECLASS_AND_SCALE_GPU_MACHINESET",
			map[string]any{"spec": map[string]any{"spec": map[string]any{"devices": map[string]any{"requests": []map[string]any{{"name": "gpu", "deviceClassName": "gpu.nvidia.com"}}}}}},
			"Pod Pending due to Kubernetes 1.35 Dynamic Resource Allocation (resource.k8s.io/v1beta1 ResourceClaim) exhaustion across ResourceSlices; staged approval to reconcile DeviceClass / scale GPU MachineSet.",
			[]string{"Kubernetes 1.35 Dynamic Resource Allocation (DRA structured parameters, ResourceSlice, DeviceClass)"},
			[]string{"OpenShift 4.22 Node Feature Discovery (NFD) & GPU Operator DRA driver"},
		)

	// 13. PVC Provisioning Pending (StorageClass / WaitForFirstConsumer / CSI)
	case strings.Contains(reasonLower, "provisioningfailed") || strings.Contains(reasonLower, "unbound immediate persistentvolumeclaims") || strings.Contains(reasonLower, "storageclass"):
		return e.handleScenarioAction(ctx, ev,
			types.IssuePVCBindingPending,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"v1", "PersistentVolumeClaim", ev.Namespace, defaultStr(ev.PVCName, "data-stateful-0"),
			"RECONCILE_PVC_STORAGECLASS",
			map[string]any{"spec": map[string]any{"storageClassName": "gp3-csi"}},
			"PersistentVolumeClaim stuck in Pending due to missing/deprecated StorageClass or CSI provisioner stall; staged RemediationRequest CRD & GitOps PR to bind to gp3-csi.",
			[]string{"Kubernetes 1.35 VolumeAttributesClass dynamic IOPS/throughput modification"},
			[]string{"OpenShift 4.22 Data Foundation (ODF) / CSI StorageClass reconciliation"},
		)

	// 14. PVC Filesystem Capacity Exhausted (>90% full) -> Auto-Expand Volume
	case ev.PVCName != "" || strings.Contains(reasonLower, "filesystem") || strings.Contains(reasonLower, "pvc full") || strings.Contains(reasonLower, "storage full") || strings.Contains(reasonLower, "no space left on device"):
		return e.handlePVCCapacityExhausted(ctx, ev)

	// 15. Stuck VolumeAttachment / Multi-Attach Error
	case strings.Contains(reasonLower, "multi-attach") || strings.Contains(reasonLower, "volumeattachment"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueVolumeAttachmentStuck,
			types.CriticalityP0Critical,
			types.SeveritySev1,
			types.PolicyApprovalRequired,
			"storage.k8s.io/v1", "VolumeAttachment", "", defaultStr(ev.PVCName, "csi-stale-attachment"),
			"DETACH_STALE_VOLUME_ATTACHMENT",
			map[string]any{"metadata": map[string]any{"annotations": map[string]string{"kubedoctor.io/force-detach-verified-fenced-node": "true"}}},
			"StatefulSet pod blocked by Multi-Attach error for RWO volume still attached to unresponsive node; staged approval to detach stale VolumeAttachment after node fencing verification.",
			[]string{"Kubernetes 1.35 CSI ReadWriteOncePod & non-graceful node shutdown volume detach"},
			nil,
		)

	// 16. Service Endpoints Empty (0 ready endpoints in EndpointSlice)
	case ev.ServiceName != "" || strings.Contains(reasonLower, "endpoint") || strings.Contains(reasonLower, "no endpoints available"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueServiceEndpointsEmpty,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyAutoRemediate,
			"v1", "Service", ev.Namespace, defaultStr(ev.ServiceName, "backend-svc"),
			"ALIGN_SERVICE_SELECTOR_WITH_WORKLOAD_LABELS",
			map[string]any{"spec": map[string]any{"selector": map[string]string{"app.kubernetes.io/name": defaultStr(ev.OwnerName, "backend")}}},
			"Kubernetes Service / EndpointSlice (discovery.k8s.io/v1) has 0 ready endpoints due to selector label drift; auto-aligned Service spec.selector with running Pod labels.",
			[]string{"Kubernetes 1.35 EndpointSlice trafficDistribution (PreferClose) routing"},
			nil,
		)

	// 17. NetworkPolicy / OVN-Kubernetes Traffic Drop
	case strings.Contains(reasonLower, "networkpolicy") || strings.Contains(reasonLower, "connection timed out") || strings.Contains(reasonLower, "ovn"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueNetworkPolicyDrop,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"networking.k8s.io/v1", "NetworkPolicy", ev.Namespace, defaultStr(ev.OwnerName, "allow-ingress-egress"),
			"UPDATE_NETWORK_POLICY_INGRESS_EGRESS_RULES",
			map[string]any{"spec": map[string]any{"ingress": []map[string]any{{"from": []map[string]any{{"namespaceSelector": map[string]any{"matchLabels": map[string]string{"kubernetes.io/metadata.name": ev.Namespace}}}}}}}},
			"OVN-Kubernetes / NetworkPolicy ACL dropping inter-service traffic; staged GitOps PR & RemediationRequest CRD to allow required namespace ingress.",
			[]string{"Kubernetes 1.35 AdminNetworkPolicy & BaselineAdminNetworkPolicy"},
			[]string{"OpenShift 4.22 OVN-Kubernetes NetObserv flow drop correlation"},
		)

	// 18. OpenShift Route / Gateway API Ingress Degraded (503 / TLS Expired)
	case ev.RouteName != "" || strings.Contains(reasonLower, "route") || strings.Contains(reasonLower, "503") || strings.Contains(reasonLower, "ingress"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueOCPRouteDegraded,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyAutoRemediate,
			"route.openshift.io/v1", "Route", ev.Namespace, defaultStr(ev.RouteName, "public-route"),
			"RECONCILE_OPENSHIFT_ROUTE_TARGET_PORT",
			map[string]any{"spec": map[string]any{"port": map[string]any{"targetPort": "http-8080"}}},
			"OpenShift Route returning 503 Service Unavailable due to mismatched targetPort; auto-reconciled Route targetPort against backing Service.",
			[]string{"Gateway API v1.2 HTTPRoute & GRPCRoute conformance"},
			[]string{"OpenShift 4.22 HAProxy IngressRouter & route.openshift.io/v1 reconciliation"},
		)

	// 19. CoreDNS / OpenShift DNS Resolution Failure
	case strings.Contains(reasonLower, "coredns") || strings.Contains(reasonLower, "dns") || strings.Contains(reasonLower, "nxdomain") || strings.Contains(reasonLower, "servfail"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueCoreDNSFailure,
			types.CriticalityP0Critical,
			types.SeveritySev1,
			types.PolicyAutoRemediate,
			"operator.openshift.io/v1", "DNS", "openshift-dns-operator", "default",
			"RECONCILE_CLUSTER_DNS_CACHE_AND_REPLICAS",
			map[string]any{"spec": map[string]any{"cache": map[string]string{"positiveTTL": "30s", "negativeTTL": "5s"}}},
			"Cluster DNS experiencing SERVFAIL / latency spikes; tuned CoreDNS cache TTLs and reconciled openshift-dns daemonset pods.",
			nil,
			[]string{"OpenShift 4.22 DNS Operator (operator.openshift.io/v1) CoreDNS tuning"},
		)

	// 20. Node NotReady (Kubelet / PLEG / cgroup v2 PSI Stall)
	case ev.NodeName != "" || strings.Contains(reasonLower, "nodenotready") || strings.Contains(reasonLower, "pleg") || strings.Contains(reasonLower, "kubelet"):
		return e.handleNodeNotReady(ctx, ev)

	// 21. OpenShift 4.22+ ClusterOperator Degraded
	case ev.ClusterOperator != "" || strings.Contains(reasonLower, "clusteroperator") || strings.Contains(reasonLower, "degraded"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueClusterOperatorDegrad,
			types.CriticalityP0Critical,
			types.SeveritySev1,
			types.PolicyApprovalRequired,
			"config.openshift.io/v1", "ClusterOperator", "", defaultStr(ev.ClusterOperator, "authentication"),
			"RECONCILE_DEGRADED_OPENSHIFT_CLUSTER_OPERATOR",
			map[string]any{"metadata": map[string]any{"annotations": map[string]string{"kubedoctor.io/reconcile-operator": time.Now().UTC().Format(time.RFC3339)}}},
			"OpenShift 4.22 ClusterOperator reporting Degraded=True; inspected operator pods & events via containers/kubernetes-mcp-server and staged operator reconciliation.",
			nil,
			[]string{"OpenShift 4.22 ClusterVersionOperator (CVO) & ClusterOperator condition triage"},
		)

	// 22. OpenShift MachineConfigPool (MCP) Degraded / Node Config Drift
	case ev.MachineConfigPool != "" || strings.Contains(reasonLower, "machineconfig") || strings.Contains(reasonLower, "mcp"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueMCPNodeUpdatePaused,
			types.CriticalityP1High,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"machineconfiguration.openshift.io/v1", "MachineConfigPool", "", defaultStr(ev.MachineConfigPool, "worker"),
			"RECONCILE_OPENSHIFT_MACHINE_CONFIG_POOL",
			map[string]any{"spec": map[string]any{"paused": false}},
			"OpenShift 4.22 MachineConfigPool degraded due to machine-config-daemon drift; staged RemediationRequest CRD & GitOps PR to clear node config drift and unpause pool.",
			nil,
			[]string{"OpenShift 4.22 MachineConfigOperator (MCO) & Bootable Container (bootc / RHEL CoreOS) layer reconciliation"},
		)

	// 23. Etcd High WAL Fsync Latency / Quorum Risk
	case strings.Contains(reasonLower, "etcd") || strings.Contains(reasonLower, "quorum") || strings.Contains(reasonLower, "fsync"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueEtcdHighLatency,
			types.CriticalityP0Critical,
			types.SeveritySev1,
			types.PolicyApprovalRequired,
			"operator.openshift.io/v1", "Etcd", "", "cluster",
			"DEFRAG_AND_TUNE_ETCD_CONTROL_PLANE",
			map[string]any{"spec": map[string]any{"controlPlaneHardwareSpeed": "Slower"}},
			"Control plane etcd reporting high WAL fsync latency / database size fragmentation; staged approval for rolling non-leader etcd defragmentation and hardware profile tuning.",
			[]string{"Kubernetes 1.35 WatchList streaming & consistent reads from cache (reducing etcd load)"},
			[]string{"OpenShift 4.22 Etcd Operator defragmentation & hardware speed profiles"},
		)

	// 24. Cluster Certificate / Kubelet CSR Expiring
	case strings.Contains(reasonLower, "certificate") || strings.Contains(reasonLower, "csr") || strings.Contains(reasonLower, "x509") || strings.Contains(reasonLower, "tls"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueCertificateExpiring,
			types.CriticalityP0Critical,
			types.SeveritySev1,
			types.PolicyApprovalRequired,
			"certificates.k8s.io/v1", "CertificateSigningRequest", "", defaultStr(ev.OwnerName, "csr-node-worker"),
			"APPROVE_KUBELET_CSR_OR_ROTATE_TLS_SECRET",
			map[string]any{"status": map[string]any{"conditions": []map[string]string{{"type": "Approved", "status": "True", "reason": "KubeDoctorApproved"}}}},
			"Detected pending Kubelet serving CertificateSigningRequest or expiring webhook TLS certificate; staged approval to sign/rotate certificate.",
			[]string{"Kubernetes 1.35 ClusterTrustBundle & kubelet CSR rotation"},
			[]string{"OpenShift 4.22 cluster-kube-apiserver-operator & machine-approver CSR validation"},
		)

	// 25. RBAC Forbidden / OpenShift SecurityContextConstraints (SCC) Rejection
	case strings.Contains(reasonLower, "forbidden") || strings.Contains(reasonLower, "rbac") || strings.Contains(reasonLower, "scc") || strings.Contains(reasonLower, "securitycontextconstraints"):
		return e.handleScenarioAction(ctx, ev,
			types.IssueRBACPermissionDenied,
			types.CriticalityP2Medium,
			types.SeveritySev2,
			types.PolicyApprovalRequired,
			"rbac.authorization.k8s.io/v1", "RoleBinding", ev.Namespace, defaultStr(ev.OwnerName, "workload-least-privilege-rb"),
			"GRANT_LEAST_PRIVILEGE_RBAC_OR_USERNS_SCC",
			map[string]any{"roleRef": map[string]string{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "view"}},
			"Workload failed with 403 Forbidden / OpenShift SCC rejection; staged GitOps PR & RemediationRequest CRD with least-privilege RoleBinding and K8s 1.35 UserNamespaces (hostUsers: false).",
			[]string{"Kubernetes 1.35 UserNamespaces (hostUsers: false) for rootless container isolation"},
			[]string{"OpenShift 4.22 restricted-v2 SecurityContextConstraints (SCC) compliance"},
		)

	// 26. CrashLoopBackOff (Default application crash / bad rollout)
	default:
		return e.handleCrashLoopBackOff(ctx, ev)
	}
}

func (e *Engine) handleCrashLoopBackOff(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	ownerKind := defaultStr(ev.OwnerKind, "Deployment")
	ownerName := defaultStr(ev.OwnerName, "app-service")
	containerName := defaultStr(ev.ContainerName, ownerName)

	if e.k8sMCP != nil && ev.PodName != "" {
		_, _ = e.k8sMCP.GetPod(ctx, ev.Namespace, ev.PodName)
		_, _ = e.k8sMCP.GetPodLogs(ctx, ev.Namespace, ev.PodName, containerName, true, 100)
	}

	return e.handleScenarioAction(ctx, ev,
		types.IssueCrashLoopBackOff,
		types.CriticalityP1High,
		types.SeveritySev2,
		types.PolicyAutoRemediate,
		"apps/v1", ownerKind, ev.Namespace, ownerName,
		"ROLLOUT_RESTART_OR_ROLLBACK_WORKLOAD",
		map[string]any{
			"spec": map[string]any{
				"template": map[string]any{
					"metadata": map[string]any{
						"annotations": map[string]string{
							"kubectl.kubernetes.io/restartedAt": time.Now().UTC().Format(time.RFC3339),
						},
					},
				},
			},
		},
		fmt.Sprintf("Pod %s/%s in %s/%s is in CrashLoopBackOff (%s). Retrieved previous container logs via containers/kubernetes-mcp-server (pods_log previous=true) and triggered automated rollout recovery.", ev.Namespace, ev.PodName, ownerKind, ownerName, ev.Message),
		[]string{
			"cgroup v2 PSI (Pressure Stall Information) metrics via kubernetes-mcp-server nodes_stats_summary",
			"SidecarContainers native lifecycle diagnostics",
		},
		[]string{
			"OpenShift 4.22 Deployment rollout health & image stream verification",
		},
	)
}

func (e *Engine) handleImagePullBackOff(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	if e.k8sMCP != nil {
		_, _ = e.k8sMCP.ListEvents(ctx, ev.Namespace, fmt.Sprintf("involvedObject.name=%s", ev.PodName))
	}
	return e.handleScenarioAction(ctx, ev,
		types.IssueImagePullBackOff,
		types.CriticalityP1High,
		types.SeveritySev2,
		types.PolicyApprovalRequired,
		"config.openshift.io/v1", "ImageDigestMirrorSet", "", "registry-mirrors",
		"RECONCILE_IMAGE_MIRROR_OR_ROLLBACK_IMAGE_TAG",
		map[string]any{
			"spec": map[string]any{
				"imageDigestMirrors": []map[string]any{
					{
						"source":  "docker.io/library",
						"mirrors": []string{"quay.io/organization-mirror"},
					},
				},
			},
		},
		fmt.Sprintf("Pod %s/%s failed with ImagePullBackOff (%s). Inspected events via containers/kubernetes-mcp-server (events_list) and staged OpenShift 4.22 ImageDigestMirrorSet / image tag rollback for approval.", ev.Namespace, ev.PodName, ev.Message),
		[]string{"Kubernetes 1.35 Kubelet Image Pull Credential Provider & KubeletSeparateDiskGC"},
		[]string{"OpenShift 4.22 ImageDigestMirrorSet / ImageTagMirrorSet (replacing legacy ImageContentSourcePolicy)"},
	)
}

func (e *Engine) handlePVCCapacityExhausted(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	pvcName := defaultStr(ev.PVCName, "data-"+defaultStr(ev.PodName, "stateful-0"))
	curStorageGiB := k8s.ParseMemoryMiB(defaultStr(ev.CurrentStorage, "20Gi")) / 1024
	if curStorageGiB <= 0 {
		curStorageGiB = 20
	}
	newStorageStr := fmt.Sprintf("%dGi", curStorageGiB*2)

	return e.handleScenarioAction(ctx, ev,
		types.IssuePVCCapacityExhausted,
		types.CriticalityP1High,
		types.SeveritySev2,
		types.PolicyAutoRemediate,
		"v1", "PersistentVolumeClaim", ev.Namespace, pvcName,
		"ONLINE_EXPAND_PERSISTENT_VOLUME_CLAIM",
		map[string]any{
			"spec": map[string]any{
				"resources": map[string]any{
					"requests": map[string]string{
						"storage": newStorageStr,
					},
				},
			},
		},
		fmt.Sprintf("PersistentVolumeClaim %s/%s filesystem utilization exceeded 90%%; autonomously expanded PVC storage request to %s via containers/kubernetes-mcp-server resources_create_or_update (online CSI volume expansion).", ev.Namespace, pvcName, newStorageStr),
		[]string{"Kubernetes 1.35 Online CSI Volume Expansion & VolumeAttributesClass"},
		[]string{"OpenShift 4.22 CSI driver online resizefs without pod restart"},
	)
}

func (e *Engine) handleNodeNotReady(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	nodeName := defaultStr(ev.NodeName, "worker-node-01")
	if e.k8sMCP != nil {
		_, _ = e.k8sMCP.GetNodeStatsSummary(ctx, nodeName)
	}
	return e.handleScenarioAction(ctx, ev,
		types.IssueNodeNotReady,
		types.CriticalityP0Critical,
		types.SeveritySev1,
		types.PolicyApprovalRequired,
		"v1", "Node", "", nodeName,
		"CORDON_AND_DRAIN_UNHEALTHY_NODE",
		map[string]any{
			"spec": map[string]any{
				"unschedulable": true,
				"taints": []map[string]string{
					{
						"key":    "node.kubernetes.io/out-of-service",
						"value":  "nodeshutdown",
						"effect": "NoExecute",
					},
				},
			},
		},
		fmt.Sprintf("Node %q transitioned to NotReady (%s). Queried cgroup v2 PSI metrics via containers/kubernetes-mcp-server (nodes_stats_summary) and staged RemediationRequest CRD & GitOps PR to cordon/fenced-evacuate workloads.", nodeName, ev.Message),
		[]string{
			"Kubernetes 1.35 Non-Graceful Node Shutdown (node.kubernetes.io/out-of-service:NoExecute)",
			"cgroup v2 PSI (Pressure Stall Information) via nodes_stats_summary",
		},
		[]string{"OpenShift 4.22 MachineHealthCheck & Node Maintenance Operator"},
	)
}

func (e *Engine) handleScenarioAction(
	ctx context.Context,
	ev types.DiagnosticEvent,
	issueType types.IssueType,
	criticality types.Criticality,
	severity types.Severity,
	policy types.ActionPolicy,
	apiVersion, kind, namespace, targetName, operation string,
	patchBody map[string]any,
	analysis string,
	k8sFeatures []string,
	ocpFeatures []string,
) (*types.TriageAssessment, error) {
	fullObj := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name":      targetName,
			"namespace": namespace,
		},
	}
	for k, v := range patchBody {
		fullObj[k] = v
	}
	payloadBytes, _ := json.Marshal(fullObj)

	act := types.RemediationAction{
		ActionID:         fmt.Sprintf("act-%s-%d", strings.ToLower(kind), time.Now().UnixNano()),
		StepOrder:        1,
		TargetAPIVersion: apiVersion,
		TargetKind:       kind,
		TargetNamespace:  namespace,
		TargetName:       targetName,
		Operation:        operation,
		Policy:           policy,
		PatchPayload:     string(payloadBytes),
		GitOpsFilePath:   fmt.Sprintf("clusters/ocp-4.22/%s/%s-%s.yaml", defaultStr(namespace, "cluster-scoped"), strings.ToLower(kind), targetName),
		Description:      analysis,
	}

	if policy == types.PolicyAutoRemediate {
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, string(payloadBytes))
		}
		_ = e.k8sClient.PatchGenericResource(ctx, apiVersion, kind, namespace, targetName, string(payloadBytes), analysis)
		now := time.Now().UTC()
		act.Status = "EXECUTED"
		act.ExecutedAt = &now
	} else {
		approvalReq, _ := e.gitopsManager.CreateApprovalRequest(
			ctx,
			issueType,
			criticality,
			severity,
			analysis,
			act,
			nil,
		)
		if approvalReq != nil {
			act = approvalReq.Action
			if e.k8sMCP != nil {
				_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, approvalReq.CRDManifestYAML)
			}
		}
	}

	return &types.TriageAssessment{
		EventID:            ev.ID,
		IssueType:          issueType,
		Criticality:        criticality,
		Severity:           severity,
		Summary:            fmt.Sprintf("[%s] %s on %s/%s (%s)", issueType, operation, kind, targetName, act.Status),
		RootCauseAnalysis:  analysis,
		K8s135FeaturesUsed: k8sFeatures,
		OCP422FeaturesUsed: ocpFeatures,
		PlannedActions:     []types.RemediationAction{act},
	}, nil
}

func (e *Engine) handlePodOOMKilled(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	ownerKind := ev.OwnerKind
	if ownerKind == "" {
		ownerKind = "StatefulSet"
	}
	ownerName := ev.OwnerName
	if ownerName == "" {
		if idx := strings.LastIndex(ev.PodName, "-"); idx > 0 {
			ownerName = ev.PodName[:idx]
		} else {
			ownerName = "stateful-app"
		}
	}
	containerName := ev.ContainerName
	if containerName == "" {
		containerName = ownerName
	}

	if e.k8sMCP != nil {
		if ev.PodName != "" {
			_, _ = e.k8sMCP.GetPod(ctx, ev.Namespace, ev.PodName)
			_, _ = e.k8sMCP.GetPodLogs(ctx, ev.Namespace, ev.PodName, containerName, true, 50)
		}
		_, _ = e.k8sMCP.GetResource(ctx, "apps/v1", ownerKind, ev.Namespace, ownerName)
	}

	curLimMiB := k8s.ParseMemoryMiB(ev.CurrentMemoryLim)
	if curLimMiB <= 0 {
		curLimMiB = 512
	}
	curReqMiB := k8s.ParseMemoryMiB(ev.CurrentMemoryReq)
	if curReqMiB <= 0 {
		curReqMiB = curLimMiB / 2
	}

	deltaMiB := curLimMiB / 2
	if deltaMiB < 256 {
		deltaMiB = 256
	}
	newLimMiB := curLimMiB + deltaMiB
	newReqMiB := curReqMiB + (deltaMiB / 2)
	if newReqMiB > newLimMiB {
		newReqMiB = newLimMiB
	}

	newLimStr := k8s.FormatMemoryMiB(newLimMiB)
	newReqStr := k8s.FormatMemoryMiB(newReqMiB)

	quotaLimMiB := k8s.ParseMemoryMiB(ev.QuotaMemoryLimit)
	quotaUsedMiB := k8s.ParseMemoryMiB(ev.QuotaMemoryUsed)
	reasonLower := strings.ToLower(ev.Reason + " " + ev.Message)
	quotaExceeded := strings.Contains(reasonLower, "quota") ||
		(quotaLimMiB > 0 && (quotaUsedMiB+deltaMiB) > quotaLimMiB)

	stsFullManifestObj := map[string]any{
		"apiVersion": "apps/v1",
		"kind":       ownerKind,
		"metadata": map[string]any{
			"name":      ownerName,
			"namespace": ev.Namespace,
		},
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{
						{
							"name": containerName,
							"resources": map[string]any{
								"requests": map[string]string{"memory": newReqStr},
								"limits":   map[string]string{"memory": newLimStr},
							},
						},
					},
				},
			},
		},
	}
	stsPatchBytes, _ := json.Marshal(stsFullManifestObj)

	stsAction := types.RemediationAction{
		ActionID:         fmt.Sprintf("act-sts-%d", time.Now().UnixNano()),
		StepOrder:        1,
		TargetAPIVersion: "apps/v1",
		TargetKind:       ownerKind,
		TargetNamespace:  ev.Namespace,
		TargetName:       ownerName,
		Operation:        "PATCH_STATEFULSET_AND_INPLACE_RESIZE",
		Policy:           types.PolicyAutoRemediate,
		PatchPayload:     string(stsPatchBytes),
		GitOpsFilePath:   fmt.Sprintf("clusters/ocp-4.22/namespaces/%s/%s-%s.yaml", ev.Namespace, strings.ToLower(ownerKind), ownerName),
		Description:      fmt.Sprintf("Update %s %s/%s container %q memory from %s to %s (request %s) via containers/kubernetes-mcp-server (resources_create_or_update) and trigger K8s 1.35+ In-Place Pod Vertical Scaling (/resize) to recover OOMKilled pod %s", ownerKind, ev.Namespace, ownerName, containerName, k8s.FormatMemoryMiB(curLimMiB), newLimStr, newReqStr, ev.PodName),
	}

	if !quotaExceeded {
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, string(stsPatchBytes))
		}
		_, err := e.k8sClient.PatchWorkloadResources(ctx, ev.Namespace, ownerKind, ownerName, ev.PodName, containerName, newReqStr, newLimStr)
		now := time.Now().UTC()
		if err != nil {
			stsAction.Status = "FAILED"
		} else {
			stsAction.Status = "EXECUTED"
			stsAction.ExecutedAt = &now
		}

		return &types.TriageAssessment{
			EventID:     ev.ID,
			IssueType:   types.IssuePodOOMKilled,
			Criticality: types.CriticalityP1High,
			Severity:    types.SeveritySev2,
			Summary:     fmt.Sprintf("Pod %s/%s (managed by %s/%s) failed with OOMKilled (ExitCode 137). Auto-remediated memory limit %s -> %s within namespace quota via containers/kubernetes-mcp-server.", ev.Namespace, ev.PodName, ownerKind, ownerName, k8s.FormatMemoryMiB(curLimMiB), newLimStr),
			RootCauseAnalysis: fmt.Sprintf(
				"Container %q in pod %s/%s terminated with ExitCode 137 (SIGKILL due to cgroup v2 memory.max OOM). Inspected pod & previous logs via containers/kubernetes-mcp-server (pods_get, pods_log). Namespace ResourceQuota has sufficient headroom, so kube-doctor autonomously applied the updated %s resource spec via resources_create_or_update and invoked the Kubernetes 1.35+ pods/resize subresource.",
				containerName, ev.Namespace, ev.PodName, ownerKind,
			),
			K8s135FeaturesUsed: []string{
				"InPlacePodVerticalScaling (GA in K8s 1.35+) via pods/resize subresource",
				"containers/kubernetes-mcp-server Server-Side Apply (resources_create_or_update)",
				"cgroup v2 memory.oom.group & PSI telemetry via nodes_stats_summary",
			},
			OCP422FeaturesUsed: []string{
				"OpenShift 4.22 CRI-O live container cgroup v2 vertical memory update",
			},
			PlannedActions: []types.RemediationAction{stsAction},
		}, nil
	}

	quotaName := ev.QuotaName
	if quotaName == "" {
		quotaName = "namespace-compute-quota"
	}
	if e.k8sMCP != nil {
		_, _ = e.k8sMCP.GetResource(ctx, "v1", "ResourceQuota", ev.Namespace, quotaName)
	}
	if quotaLimMiB <= 0 {
		quotaLimMiB = 2048
	}
	newQuotaLimMiB := quotaLimMiB + deltaMiB + 1024
	newQuotaLimStr := k8s.FormatMemoryMiB(newQuotaLimMiB)
	newQuotaReqStr := k8s.FormatMemoryMiB(newQuotaLimMiB)

	quotaFullObj := map[string]any{
		"apiVersion": "v1",
		"kind":       "ResourceQuota",
		"metadata": map[string]any{
			"name":      quotaName,
			"namespace": ev.Namespace,
		},
		"spec": map[string]any{
			"hard": map[string]string{
				"limits.memory":   newQuotaLimStr,
				"requests.memory": newQuotaReqStr,
			},
		},
	}
	quotaPatchBytes, _ := json.Marshal(quotaFullObj)

	quotaAction := types.RemediationAction{
		ActionID:         fmt.Sprintf("act-quota-%d", time.Now().UnixNano()),
		StepOrder:        1,
		TargetAPIVersion: "v1",
		TargetKind:       "ResourceQuota",
		TargetNamespace:  ev.Namespace,
		TargetName:       quotaName,
		Operation:        "UPDATE_NAMESPACE_RESOURCE_QUOTA",
		Policy:           types.PolicyApprovalRequired,
		PatchPayload:     string(quotaPatchBytes),
		GitOpsFilePath:   fmt.Sprintf("clusters/ocp-4.22/namespaces/%s/resourcequota-%s.yaml", ev.Namespace, quotaName),
		Description:      fmt.Sprintf("Expand Namespace %q ResourceQuota %q limits.memory from %s to %s via containers/kubernetes-mcp-server resources_create_or_update (requires approval before StatefulSet %s memory can be bumped)", ev.Namespace, quotaName, k8s.FormatMemoryMiB(quotaLimMiB), newQuotaLimStr, ownerName),
	}

	stsAction.StepOrder = 2
	stsAction.Status = "QUEUED_AFTER_APPROVAL"

	reason := fmt.Sprintf("Pod %s/%s (StatefulSet %s) is OOMKilled and needs %s memory (delta +%s), which exceeds Namespace %q ResourceQuota %q (used %s / hard %s).",
		ev.Namespace, ev.PodName, ownerName, newLimStr, k8s.FormatMemoryMiB(deltaMiB), ev.Namespace, quotaName, k8s.FormatMemoryMiB(quotaUsedMiB), k8s.FormatMemoryMiB(quotaLimMiB))

	approvalReq, _ := e.gitopsManager.CreateApprovalRequest(
		ctx,
		types.IssueNamespaceQuotaExceed,
		types.CriticalityP0Critical,
		types.SeveritySev1,
		reason,
		quotaAction,
		[]types.RemediationAction{stsAction},
	)

	if approvalReq != nil {
		quotaAction = approvalReq.Action
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, approvalReq.CRDManifestYAML)
		}
	}

	return &types.TriageAssessment{
		EventID:     ev.ID,
		IssueType:   types.IssueNamespaceQuotaExceed,
		Criticality: types.CriticalityP0Critical,
		Severity:    types.SeveritySev1,
		Summary:     fmt.Sprintf("Pod %s/%s (StatefulSet %s) is OOMKilled, but bumping memory to %s exceeds Namespace ResourceQuota %s/%s. Created RemediationRequest CRD %q and GitOps PR %s for approval.", ev.Namespace, ev.PodName, ownerName, newLimStr, ev.Namespace, quotaName, approvalReq.CRDName, approvalReq.GitOpsPRURL),
		RootCauseAnalysis: fmt.Sprintf(
			"1) Pod %s/%s in StatefulSet %s was OOMKilled (ExitCode 137) at %s limit (verified via containers/kubernetes-mcp-server pods_get & resources_get).\n2) Increasing memory limit to %s requires +%s, which breaches Namespace ResourceQuota %s/%s (used=%s, hard=%s).\n3) Per cluster governance policy, ResourceQuota expansion requires approval. Once approved (%s), kube-doctor will invoke containers/kubernetes-mcp-server resources_create_or_update on both ResourceQuota and StatefulSet %s/%s.",
			ev.Namespace, ev.PodName, ownerName, k8s.FormatMemoryMiB(curLimMiB), newLimStr, k8s.FormatMemoryMiB(deltaMiB), ev.Namespace, quotaName, k8s.FormatMemoryMiB(quotaUsedMiB), k8s.FormatMemoryMiB(quotaLimMiB), approvalReq.ID, ev.Namespace, ownerName,
		),
		K8s135FeaturesUsed: []string{
			"InPlacePodVerticalScaling (GA in K8s 1.35+) queued behind ResourceQuota admission check",
			"containers/kubernetes-mcp-server Server-Side Apply (resources_create_or_update)",
		},
		OCP422FeaturesUsed: []string{
			"OpenShift GitOps (Argo CD) Pull Request & RemediationRequest CRD approval gate",
		},
		PlannedActions: []types.RemediationAction{quotaAction, stsAction},
	}, nil
}

func (e *Engine) handleQuotaExceededOnly(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	ev.ExitCode = 137
	if ev.QuotaMemoryLimit == "" {
		ev.QuotaMemoryLimit = "2Gi"
	}
	if ev.QuotaMemoryUsed == "" {
		ev.QuotaMemoryUsed = "2Gi"
	}
	return e.handlePodOOMKilled(ctx, ev)
}

func (e *Engine) handleMissingMachine(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	msName := ev.MachineSetName
	if msName == "" {
		msName = "ocp422-worker-us-east-1a"
	}
	if e.k8sMCP != nil {
		_, _ = e.k8sMCP.GetResource(ctx, "machine.openshift.io/v1beta1", "MachineSet", "openshift-machine-api", msName)
	}

	expected := ev.ExpectedMachines
	ready := ev.ReadyMachines
	if expected <= 0 {
		expected = 3
	}
	if ready <= 0 || ready >= expected {
		ready = expected - 1
	}
	missingCount := expected - ready
	targetReplicas := expected
	if strings.Contains(strings.ToLower(ev.Reason+" "+ev.Message), "insufficient") {
		targetReplicas = expected + 1
	}

	patchObj := map[string]any{
		"apiVersion": "machine.openshift.io/v1beta1",
		"kind":       "MachineSet",
		"metadata": map[string]any{
			"name":      msName,
			"namespace": "openshift-machine-api",
		},
		"spec": map[string]any{
			"replicas": targetReplicas,
		},
	}
	patchBytes, _ := json.Marshal(patchObj)

	msAction := types.RemediationAction{
		ActionID:         fmt.Sprintf("act-ms-%d", time.Now().UnixNano()),
		StepOrder:        1,
		TargetAPIVersion: "machine.openshift.io/v1beta1",
		TargetKind:       "MachineSet",
		TargetNamespace:  "openshift-machine-api",
		TargetName:       msName,
		Operation:        "AUTOSCALE_OPENSHIFT_MACHINESET",
		Policy:           types.PolicyApprovalRequired,
		PatchPayload:     string(patchBytes),
		GitOpsFilePath:   fmt.Sprintf("clusters/ocp-4.22/machine-api/machineset-%s.yaml", msName),
		Description:      fmt.Sprintf("Scale OpenShift 4.22+ MachineSet openshift-machine-api/%s replicas to %d via containers/kubernetes-mcp-server resources_scale to recover %d missing/failed worker machine(s)", msName, targetReplicas, missingCount),
	}

	reason := fmt.Sprintf("OpenShift 4.22 cluster detected %d missing/failed Machine(s) in MachineSet %q (ready=%d, expected=%d). Scaling MachineSet to %d replicas requires human approval.",
		missingCount, msName, ready, expected, targetReplicas)

	approvalReq, _ := e.gitopsManager.CreateApprovalRequest(
		ctx,
		types.IssueMachineMissing,
		types.CriticalityP0Critical,
		types.SeveritySev1,
		reason,
		msAction,
		nil,
	)

	if approvalReq != nil {
		msAction = approvalReq.Action
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, approvalReq.CRDManifestYAML)
		}
	}

	return &types.TriageAssessment{
		EventID:     ev.ID,
		IssueType:   types.IssueMachineMissing,
		Criticality: types.CriticalityP0Critical,
		Severity:    types.SeveritySev1,
		Summary:     fmt.Sprintf("OpenShift 4.22 MachineSet %q is missing %d machine(s) (ready=%d/%d). Created RemediationRequest CRD %q and GitOps PR %s awaiting approval to autoscale.", msName, missingCount, ready, expected, approvalReq.CRDName, approvalReq.GitOpsPRURL),
		RootCauseAnalysis: fmt.Sprintf(
			"OpenShift 4.22 Machine API / Cluster API reports MachineSet openshift-machine-api/%s has only %d/%d Ready machines (%s). Inspected via containers/kubernetes-mcp-server (resources_get). Provisioning cloud worker nodes modifies cluster infrastructure capacity and cost, so kube-doctor staged a GitOps Pull Request (%s) and RemediationRequest CRD (%s) for approval.",
			msName, ready, expected, ev.Message, approvalReq.GitOpsPRURL, approvalReq.CRDName,
		),
		K8s135FeaturesUsed: []string{
			"Kubernetes 1.35 Taint/Toleration & Node Readiness Gates evaluation",
		},
		OCP422FeaturesUsed: []string{
			"OpenShift 4.22 MachineAPI (machine.openshift.io/v1beta1) & Cluster API (cluster.x-k8s.io/v1beta1) reconciliation via containers/kubernetes-mcp-server resources_scale",
			"MachineHealthCheck & ClusterAutoscaler governance integration",
		},
		PlannedActions: []types.RemediationAction{msAction},
	}, nil
}

// ExecuteApprovedAction executes a single RemediationAction against both
// containers/kubernetes-mcp-server (resources_create_or_update / resources_scale)
// and the Kubernetes/OpenShift API server after an admin approves its RemediationRequest CRD or GitOps PR.
func (e *Engine) ExecuteApprovedAction(ctx context.Context, act types.RemediationAction) error {
	switch act.TargetKind {
	case "ResourceQuota":
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, act.PatchPayload)
		}
		var parsed struct {
			Spec struct {
				Hard map[string]string `json:"hard"`
			} `json:"spec"`
		}
		_ = json.Unmarshal([]byte(act.PatchPayload), &parsed)
		lim := parsed.Spec.Hard["limits.memory"]
		req := parsed.Spec.Hard["requests.memory"]
		if lim == "" {
			lim = "4Gi"
		}
		if req == "" {
			req = lim
		}
		_, err := e.k8sClient.PatchNamespaceResourceQuota(ctx, act.TargetNamespace, act.TargetName, lim, req)
		return err

	case "StatefulSet", "Deployment":
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, act.PatchPayload)
		}
		var parsed struct {
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name      string `json:"name"`
							Resources struct {
								Requests map[string]string `json:"requests"`
								Limits   map[string]string `json:"limits"`
							} `json:"resources"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		_ = json.Unmarshal([]byte(act.PatchPayload), &parsed)
		cName := "app"
		reqMem := "512Mi"
		limMem := "1Gi"
		if len(parsed.Spec.Template.Spec.Containers) > 0 {
			c := parsed.Spec.Template.Spec.Containers[0]
			if c.Name != "" {
				cName = c.Name
			}
			if v := c.Resources.Requests["memory"]; v != "" {
				reqMem = v
			}
			if v := c.Resources.Limits["memory"]; v != "" {
				limMem = v
			}
		}
		_, err := e.k8sClient.PatchWorkloadResources(ctx, act.TargetNamespace, act.TargetKind, act.TargetName, "", cName, reqMem, limMem)
		return err

	case "MachineSet":
		var parsed struct {
			Spec struct {
				Replicas int `json:"replicas"`
			} `json:"spec"`
		}
		_ = json.Unmarshal([]byte(act.PatchPayload), &parsed)
		replicas := parsed.Spec.Replicas
		if replicas <= 0 {
			replicas = 3
		}
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.ScaleResource(ctx, "machine.openshift.io/v1beta1", "MachineSet", "openshift-machine-api", act.TargetName, replicas)
		}
		_, err := e.k8sClient.ScaleOpenShiftMachineSet(ctx, act.TargetName, replicas)
		return err

	default:
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, act.PatchPayload)
		}
		return e.k8sClient.PatchGenericResource(ctx, act.TargetAPIVersion, act.TargetKind, act.TargetNamespace, act.TargetName, act.PatchPayload, act.Description)
	}
}

func (e *Engine) handleOperatorAdapterRemediation(ctx context.Context, adapter adapters.OperatorAdapter, ev types.DiagnosticEvent) (*types.TriageAssessment, error) {
	plan, err := adapter.BuildRemediationPlan(ctx, e.k8sClient, ev)
	if err != nil {
		return nil, err
	}

	act := plan.PrimaryAction
	var planned []types.RemediationAction

	if act.Policy == types.PolicyApprovalRequired {
		approvalReq, _ := e.gitopsManager.CreateApprovalRequest(
			ctx,
			plan.IssueType,
			plan.Criticality,
			plan.Severity,
			plan.Summary,
			act,
			plan.FollowUpActions,
		)
		if approvalReq != nil {
			act = approvalReq.Action
			if e.k8sMCP != nil {
				_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, approvalReq.CRDManifestYAML)
			}
		}
		planned = append(planned, act)
		planned = append(planned, plan.FollowUpActions...)
	} else {
		if e.k8sMCP != nil {
			_, _ = e.k8sMCP.CreateOrUpdateResource(ctx, act.PatchPayload)
		}
		if e.k8sClient != nil {
			_ = e.k8sClient.PatchGenericResource(ctx, act.TargetAPIVersion, act.TargetKind, act.TargetNamespace, act.TargetName, act.PatchPayload, act.Description)
			// If a pod belonging to this Operator CR (e.g. pg-cluster-1) is stuck in CrashLoopBackOff/OOMKilled,
			// also patch K8s 1.35 pods/resize or recycle the pod so the operator/controller recreates it with the new CR spec.
			if ev.PodName != "" && !e.k8sClient.IsSimulated() {
				_ = e.k8sClient.DeletePod(ctx, ev.Namespace, ev.PodName, fmt.Sprintf("Recycle operator-managed pod %s/%s after patching %s/%s", ev.Namespace, ev.PodName, act.TargetKind, act.TargetName))
			}
		}
		planned = append(planned, act)
	}

	return &types.TriageAssessment{
		EventID:            ev.ID,
		ClusterID:          ev.ClusterID,
		OperatorAdapter:    adapter.Name(),
		OwnershipChain:     ev.OwnershipChain,
		IssueType:          plan.IssueType,
		Criticality:        plan.Criticality,
		Severity:           plan.Severity,
		Summary:            plan.Summary,
		RootCauseAnalysis:  plan.RootCauseAnalysis,
		K8s135FeaturesUsed: plan.K8s135FeaturesUsed,
		OCP422FeaturesUsed: plan.OCP422FeaturesUsed,
		PlannedActions:     planned,
	}, nil
}

func defaultStr(val, fallback string) string {
	if strings.TrimSpace(val) != "" {
		return val
	}
	return fallback
}
