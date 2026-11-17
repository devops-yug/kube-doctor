package types

import "time"

// Criticality represents the business/operational criticality of a cluster issue.
type Criticality string

const (
	CriticalityP0Critical Criticality = "P0-CRITICAL"
	CriticalityP1High     Criticality = "P1-HIGH"
	CriticalityP2Medium   Criticality = "P2-MEDIUM"
	CriticalityP3Low      Criticality = "P3-LOW"
)

// Severity represents the technical severity score (SEV-1 to SEV-4).
type Severity string

const (
	SeveritySev1 Severity = "SEV-1"
	SeveritySev2 Severity = "SEV-2"
	SeveritySev3 Severity = "SEV-3"
	SeveritySev4 Severity = "SEV-4"
)

// ActionPolicy defines whether an action can be executed autonomously or requires approval.
type ActionPolicy string

const (
	PolicyAutoRemediate    ActionPolicy = "AUTO_REMEDIATE"
	PolicyApprovalRequired ActionPolicy = "APPROVAL_REQUIRED"
	PolicyObserveOnly      ActionPolicy = "OBSERVE_ONLY"
)

// IssueType categorizes the detected Kubernetes 1.35+ / OpenShift 4.22+ failure mode.
type IssueType string

const (
	// 1. Pod & Workload Lifecycle Issues
	IssuePodOOMKilled            IssueType = "POD_OOM_KILLED"
	IssueCrashLoopBackOff        IssueType = "POD_CRASH_LOOP_BACKOFF"
	IssueImagePullBackOff        IssueType = "IMAGE_PULL_BACKOFF"
	IssueSidecarContainerFailure IssueType = "SIDECAR_CONTAINER_FAILURE"
	IssueLivenessProbeFailure    IssueType = "LIVENESS_READINESS_PROBE_FAILURE"
	IssuePodEvictedNodePressure  IssueType = "POD_EVICTED_NODE_PRESSURE"
	IssuePodStuckTerminating     IssueType = "POD_STUCK_TERMINATING"

	// 2. Scheduling, Quota & Resource Governance Issues
	IssueNamespaceQuotaExceed IssueType = "NAMESPACE_QUOTA_EXCEEDED"
	IssueLimitRangeViolation  IssueType = "LIMIT_RANGE_VIOLATION"
	IssueHPAMaxedOut          IssueType = "HPA_MAX_REPLICAS_REACHED"
	IssuePDBBlockingDrain     IssueType = "PDB_BLOCKING_DRAIN"
	IssueDRAAllocationFailure IssueType = "DRA_ALLOCATION_FAILURE"

	// 3. Storage & Stateful Volume Issues
	IssuePVCBindingPending     IssueType = "PVC_PROVISIONING_PENDING"
	IssuePVCCapacityExhausted  IssueType = "PVC_FILESYSTEM_FULL"
	IssueVolumeAttachmentStuck IssueType = "VOLUME_ATTACHMENT_STUCK_MULTI_ATTACH"

	// 4. Networking, Ingress & Service Mesh Issues
	IssueServiceEndpointsEmpty IssueType = "SERVICE_ENDPOINTS_EMPTY"
	IssueNetworkPolicyDrop     IssueType = "NETWORK_POLICY_TRAFFIC_BLOCKED"
	IssueOCPRouteDegraded      IssueType = "OCP_ROUTE_INGRESS_DEGRADED"
	IssueCoreDNSFailure        IssueType = "COREDNS_OR_OCP_DNS_FAILURE"

	// 5. Node, OpenShift 4.22+ Infrastructure & Control Plane Issues
	IssueMachineMissing        IssueType = "OPENSHIFT_MACHINE_MISSING"
	IssueNodeNotReady          IssueType = "NODE_NOT_READY"
	IssueClusterOperatorDegrad IssueType = "OCP_CLUSTER_OPERATOR_DEGRADED"
	IssueMCPNodeUpdatePaused   IssueType = "OCP_MACHINE_CONFIG_POOL_DEGRADED"
	IssueEtcdHighLatency       IssueType = "ETCD_QUORUM_OR_LATENCY_CRITICAL"
	IssueCertificateExpiring   IssueType = "CLUSTER_CERTIFICATE_EXPIRING"
	IssueRBACPermissionDenied  IssueType = "RBAC_FORBIDDEN_ERROR"

	// 6. Custom Resource / Operator-Managed Workload Issues (CNPG, NooBaa/ODF, Strimzi, etc.)
	IssueOperatorCRDegraded IssueType = "OPERATOR_CUSTOM_RESOURCE_DEGRADED"
)

// AllIssueTypes returns the complete catalog of supported Kubernetes 1.35+ / OpenShift 4.22+ issue types.
func AllIssueTypes() []IssueType {
	return []IssueType{
		IssuePodOOMKilled,
		IssueCrashLoopBackOff,
		IssueImagePullBackOff,
		IssueSidecarContainerFailure,
		IssueLivenessProbeFailure,
		IssuePodEvictedNodePressure,
		IssuePodStuckTerminating,
		IssueNamespaceQuotaExceed,
		IssueLimitRangeViolation,
		IssueHPAMaxedOut,
		IssuePDBBlockingDrain,
		IssueDRAAllocationFailure,
		IssuePVCBindingPending,
		IssuePVCCapacityExhausted,
		IssueVolumeAttachmentStuck,
		IssueServiceEndpointsEmpty,
		IssueNetworkPolicyDrop,
		IssueOCPRouteDegraded,
		IssueCoreDNSFailure,
		IssueMachineMissing,
		IssueNodeNotReady,
		IssueClusterOperatorDegrad,
		IssueMCPNodeUpdatePaused,
		IssueEtcdHighLatency,
		IssueCertificateExpiring,
		IssueRBACPermissionDenied,
		IssueOperatorCRDegraded,
	}
}

// OwnerChainLink represents a single hop in a multi-level Kubernetes/OpenShift
// ownerReferences or operator Custom Resource hierarchy (e.g., Pod -> StatefulSet -> NooBaa -> StorageCluster,
// or Pod -> Cluster.postgresql.cnpg.io).
type OwnerChainLink struct {
	Depth      int               `json:"depth"`                // 0 = leaf or immediate owner, N = root controller CR
	APIVersion string            `json:"apiVersion"`           // e.g., "postgresql.cnpg.io/v1", "noobaa.io/v1alpha1", "ocs.openshift.io/v1"
	Kind       string            `json:"kind"`                 // e.g., "Cluster", "NooBaa", "BackingStore", "StorageCluster"
	Name       string            `json:"name"`
	Namespace  string            `json:"namespace,omitempty"`
	UID        string            `json:"uid,omitempty"`
	Controller bool              `json:"controller"`
	Relation   string            `json:"relation,omitempty"`   // "ownerReference", "labelSelector", "specReference", "subResource"
	Phase      string            `json:"phase,omitempty"`      // extracted from .status.phase
	Conditions []string          `json:"conditions,omitempty"` // degraded/failing/ready conditions from .status.conditions
	SpecFields map[string]string `json:"specFields,omitempty"` // key spec fields (e.g., memory limit, storage size, instances)
}

// ResourceHierarchyTree represents the complete multi-hop ownership & child CR topology
// for any Kubernetes/OpenShift workload or operator Custom Resource.
type ResourceHierarchyTree struct {
	ClusterID       string           `json:"clusterId,omitempty"`
	Namespace       string           `json:"namespace"`
	SeedAPIVersion  string           `json:"seedApiVersion"`
	SeedKind        string           `json:"seedKind"`
	SeedName        string           `json:"seedName"`
	OperatorAdapter string           `json:"operatorAdapter,omitempty"` // e.g., "cloudnative-pg", "noobaa-odf"
	RootController  *OwnerChainLink  `json:"rootController,omitempty"`
	OwnershipChain  []OwnerChainLink `json:"ownershipChain"`            // ordered leaf -> root
	RelatedCRs      []OwnerChainLink `json:"relatedCustomResources,omitempty"` // sibling/child CRs in hierarchy (e.g. BackingStore, BucketClass, Pooler)
}

// DiagnosticEvent represents incoming diagnostic telemetry from the cluster,
// Alertmanager, OpenShift Telemeter, Spoke Agent, or an external Copilot/MCP agent.
type DiagnosticEvent struct {
	ID                string            `json:"id,omitempty"`
	ClusterID         string            `json:"clusterId,omitempty"`  // e.g., "local", "prod-ocp-us-east", "dr-k8s-eu-west"
	ClusterVersion    string            `json:"clusterVersion"`       // e.g., "v1.35.2"
	OpenShiftVersion  string            `json:"openShiftVersion"`     // e.g., "4.22.1"
	Namespace         string            `json:"namespace"`
	PodName           string            `json:"podName,omitempty"`
	ContainerName     string            `json:"containerName,omitempty"`
	OwnerAPIVersion   string            `json:"ownerApiVersion,omitempty"` // e.g., "postgresql.cnpg.io/v1", "apps/v1"
	OwnerKind         string            `json:"ownerKind,omitempty"`       // e.g., "Cluster", "NooBaa", "StatefulSet", "Deployment"
	OwnerName         string            `json:"ownerName,omitempty"`
	OperatorAdapter   string            `json:"operatorAdapter,omitempty"` // e.g., "cloudnative-pg", "noobaa-odf"
	OwnershipChain    []OwnerChainLink  `json:"ownershipChain,omitempty"`  // full multi-hop hierarchy (e.g. 5 CRs for NooBaa)
	NodeName          string            `json:"nodeName,omitempty"`
	PVCName           string            `json:"pvcName,omitempty"`
	ServiceName       string            `json:"serviceName,omitempty"`
	RouteName         string            `json:"routeName,omitempty"`
	HPAName           string            `json:"hpaName,omitempty"`
	PDBName           string            `json:"pdbName,omitempty"`
	ClusterOperator   string            `json:"clusterOperator,omitempty"`
	MachineConfigPool string            `json:"machineConfigPool,omitempty"`
	ExitCode          int               `json:"exitCode,omitempty"`
	Reason            string            `json:"reason"` // e.g., "OOMKilled", "CrashLoopBackOff", "ImagePullBackOff", "NodeNotReady"
	Message           string            `json:"message"`
	CurrentMemoryLim  string            `json:"currentMemoryLimit,omitempty"`   // e.g., "512Mi"
	CurrentMemoryReq  string            `json:"currentMemoryRequest,omitempty"` // e.g., "256Mi"
	CurrentStorage    string            `json:"currentStorage,omitempty"`       // e.g., "20Gi"
	QuotaMemoryLimit  string            `json:"quotaMemoryLimit,omitempty"`     // e.g., "2Gi"
	QuotaMemoryUsed   string            `json:"quotaMemoryUsed,omitempty"`      // e.g., "1800Mi"
	QuotaName         string            `json:"quotaName,omitempty"`            // e.g., "compute-quota"
	MachineSetName    string            `json:"machineSetName,omitempty"`       // e.g., "cluster-worker-us-east-1a"
	MachineName       string            `json:"machineName,omitempty"`
	ExpectedMachines  int               `json:"expectedMachines,omitempty"`
	ReadyMachines     int               `json:"readyMachines,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Timestamp         time.Time         `json:"timestamp"`
}

// EffectivenessReport tracks post-remediation health verification, spec drift checks,
// and stability scoring (0-100) after an autonomous or approved remediation action runs.
type EffectivenessReport struct {
	VerificationStatus string    `json:"verificationStatus"` // "PENDING_VERIFICATION", "VERIFIED_HEALTHY", "SPEC_DRIFT_DETECTED", "REGRESSION_DETECTED", "AWAITING_APPROVAL"
	EffectivenessScore int       `json:"effectivenessScore"` // 0 to 100
	TargetResource     string    `json:"targetResource"`     // e.g., "Cluster/pg-cluster" or "StatefulSet/oom-demo"
	PodReadyVerified   bool      `json:"podReadyVerified"`
	SpecDriftDetected  bool      `json:"specDriftDetected"`
	RestartsSinceFix   int       `json:"restartsSinceFix"`
	Message            string    `json:"message"`
	ChecksPerformed    int       `json:"checksPerformed"`
	LastVerifiedAt     time.Time `json:"lastVerifiedAt"`
}

// TriageAssessment contains the LLM + rule-engine criticality/severity classification
// and the planned corrective actions.
type TriageAssessment struct {
	EventID            string               `json:"eventId"`
	ClusterID          string               `json:"clusterId,omitempty"`
	OperatorAdapter    string               `json:"operatorAdapter,omitempty"`
	OwnershipChain     []OwnerChainLink     `json:"ownershipChain,omitempty"`
	IssueType          IssueType            `json:"issueType"`
	Criticality        Criticality          `json:"criticality"`
	Severity           Severity             `json:"severity"`
	Summary            string               `json:"summary"`
	RootCauseAnalysis  string               `json:"rootCauseAnalysis"`
	K8s135FeaturesUsed []string             `json:"k8s135FeaturesUsed,omitempty"`
	OCP422FeaturesUsed []string             `json:"ocp422FeaturesUsed,omitempty"`
	PlannedActions     []RemediationAction  `json:"plannedActions"`
	Effectiveness      *EffectivenessReport `json:"effectiveness,omitempty"`
	ExternalInfoNeeded []ExternalInfoQuery  `json:"externalInfoNeeded,omitempty"`
}

// ExternalInfoQuery represents a request from kube-doctor LLM back to the calling
// Copilot Extension or Agent to fetch MCP or Web information.
type ExternalInfoQuery struct {
	Source      string         `json:"source"` // "mcp" or "web"
	ToolName    string         `json:"toolName"`
	Query       string         `json:"query"`
	Arguments   map[string]any `json:"arguments,omitempty"`
	Description string         `json:"description"`
}

// RemediationAction represents a concrete corrective action on the cluster or GitOps repo.
type RemediationAction struct {
	ActionID         string       `json:"actionId"`
	ClusterID        string       `json:"clusterId,omitempty"`
	StepOrder        int          `json:"stepOrder"`
	TargetAPIVersion string       `json:"targetApiVersion"` // e.g., "apps/v1", "v1", "postgresql.cnpg.io/v1", "noobaa.io/v1alpha1"
	TargetKind       string       `json:"targetKind"`       // e.g., "StatefulSet", "Cluster", "NooBaa", "BackingStore"
	TargetNamespace  string       `json:"targetNamespace"`
	TargetName       string       `json:"targetName"`
	Operation        string       `json:"operation"`
	Policy           ActionPolicy `json:"policy"`
	PatchPayload     string       `json:"patchPayload"`
	GitOpsManifest   string       `json:"gitOpsManifest,omitempty"`
	GitOpsFilePath   string       `json:"gitOpsFilePath,omitempty"`
	Description      string       `json:"description"`
	Status           string       `json:"status"` // "EXECUTED", "PENDING_APPROVAL", "APPROVED", "REJECTED", "FAILED", "DISPATCHED_TO_AGENT"
	ApprovalID       string       `json:"approvalId,omitempty"`
	GitOpsPRURL      string       `json:"gitOpsPrUrl,omitempty"`
	ExecutedAt       *time.Time   `json:"executedAt,omitempty"`
}

// ApprovalRequest represents a pending human approval backed by both a Kubernetes
// RemediationRequest CRD (kubedoctor.io/v1alpha1) and a GitOps Pull Request / Merge Request.
type ApprovalRequest struct {
	ID               string              `json:"id"`
	ClusterID        string              `json:"clusterId,omitempty"`
	CRDName          string              `json:"crdName"`
	Namespace        string              `json:"namespace"`
	IssueType        IssueType           `json:"issueType"`
	Criticality      Criticality         `json:"criticality"`
	Severity         Severity            `json:"severity"`
	Reason           string              `json:"reason"`
	ApprovalMode     string              `json:"approvalMode,omitempty"`     // "IN_CLUSTER", "GITHUB_PR", "GITLAB_MR", "HYBRID"
	GitProvider      string              `json:"gitProvider,omitempty"`      // "in-cluster", "github", "gitlab"
	InClusterAllowed bool                `json:"inClusterAllowed"`           // true when resource is managed in-cluster and can be approved directly in OpenShift Console
	Action           RemediationAction   `json:"action"`
	FollowUpActions  []RemediationAction `json:"followUpActions,omitempty"`
	GitOpsPRNumber   int                 `json:"gitOpsPrNumber,omitempty"`
	GitOpsPRURL      string              `json:"gitOpsPrUrl,omitempty"`
	GitOpsBranch     string              `json:"gitOpsBranch,omitempty"`
	CRDManifestYAML  string              `json:"crdManifestYaml"`
	Status           string              `json:"status"` // "PENDING", "APPROVED", "REJECTED"
	ApprovedBy       string              `json:"approvedBy,omitempty"`
	CreatedAt        time.Time           `json:"createdAt"`
	UpdatedAt        time.Time           `json:"updatedAt"`
}

// OpenAI Chat Completions compatible types so any Copilot extension or IDE agent can use kube-doctor.
type ChatCompletionRequest struct {
	Model           string           `json:"model"`
	Messages        []ChatMessage    `json:"messages"`
	Tools           []ChatTool       `json:"tools,omitempty"`
	ToolChoice      any              `json:"tool_choice,omitempty"`
	Stream          bool             `json:"stream,omitempty"`
	Temperature     float64          `json:"temperature,omitempty"`
	MaxTokens       int              `json:"max_tokens,omitempty"`
	DiagnosticEvent *DiagnosticEvent `json:"diagnostic_event,omitempty"`
}

type ChatMessage struct {
	Role       string     `json:"role"` // "system", "user", "assistant", "tool"
	Content    string     `json:"content"`
	Name       string     `json:"name,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
}

type ChatTool struct {
	Type     string           `json:"type"` // "function"
	Function ChatToolFunction `json:"function"`
}

type ChatToolFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type ToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"` // "function"
	Function ToolCallFunction `json:"function"`
}

type ToolCallFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type ChatCompletionResponse struct {
	ID               string                 `json:"id"`
	Object           string                 `json:"object"`
	Created          int64                  `json:"created"`
	Model            string                 `json:"model"`
	Choices          []ChatCompletionChoice `json:"choices"`
	Usage            ChatCompletionUsage    `json:"usage"`
	TriageAssessment *TriageAssessment      `json:"x_kube_doctor_triage,omitempty"`
}

type ChatCompletionChoice struct {
	Index        int         `json:"index"`
	Message      ChatMessage `json:"message"`
	FinishReason string      `json:"finish_reason"` // "stop", "tool_calls"
}

type ChatCompletionUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}
