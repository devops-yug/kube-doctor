package remediation

import (
	"context"
	"testing"

	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

type mockMCPExecutor struct{}

func (m *mockMCPExecutor) GetPod(ctx context.Context, namespace, name string) (string, error) {
	return "pod", nil
}
func (m *mockMCPExecutor) GetPodLogs(ctx context.Context, namespace, name, container string, previous bool, tail int) (string, error) {
	return "logs", nil
}
func (m *mockMCPExecutor) GetNodeStatsSummary(ctx context.Context, nodeName string) (string, error) {
	return "stats", nil
}
func (m *mockMCPExecutor) ListEvents(ctx context.Context, namespace, fieldSelector string) (string, error) {
	return "events", nil
}
func (m *mockMCPExecutor) GetResource(ctx context.Context, apiVersion, kind, namespace, name string) (string, error) {
	return "res", nil
}
func (m *mockMCPExecutor) CreateOrUpdateResource(ctx context.Context, resourceYAMLOrJSON string) (string, error) {
	return "applied", nil
}
func (m *mockMCPExecutor) ScaleResource(ctx context.Context, apiVersion, kind, namespace, name string, scale int) (string, error) {
	return "scaled", nil
}

func TestRemediationEngineAllScenariosAndDecisions(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	t.Setenv("DISABLE_UPSTREAM_LLM", "true")

	kc := k8s.NewClient()
	gm := gitops.NewManager(kc)
	mcpClient := &mockMCPExecutor{}
	eng := NewEngine(kc, gm, mcpClient)

	if eng.AdapterRegistry() == nil {
		t.Fatal("expected non-nil AdapterRegistry")
	}

	ctx := context.Background()

	cases := []struct {
		name      string
		ev        types.DiagnosticEvent
		wantIssue types.IssueType
	}{
		{
			name:      "1. Missing Machine",
			ev:        types.DiagnosticEvent{Reason: "MachineMissing", ExpectedMachines: 3, ReadyMachines: 2, Message: "insufficient capacity"},
			wantIssue: types.IssueMachineMissing,
		},
		{
			name:      "2a. OOMKilled within quota",
			ev:        types.DiagnosticEvent{PodName: "db-0", ExitCode: 137, CurrentMemoryLim: "256Mi", QuotaMemoryLimit: "4Gi", QuotaMemoryUsed: "512Mi"},
			wantIssue: types.IssuePodOOMKilled,
		},
		{
			name:      "2b. OOMKilled exceeding quota",
			ev:        types.DiagnosticEvent{PodName: "singlepod", ExitCode: 137, CurrentMemoryLim: "1Gi", CurrentMemoryReq: "1Gi", QuotaMemoryLimit: "1Gi", QuotaMemoryUsed: "1Gi"},
			wantIssue: types.IssueNamespaceQuotaExceed,
		},
		{
			name:      "3. Explicit Quota Exceeded",
			ev:        types.DiagnosticEvent{Reason: "Exceeded Quota"},
			wantIssue: types.IssueNamespaceQuotaExceed,
		},
		{
			name:      "4. LimitRange Violation",
			ev:        types.DiagnosticEvent{Reason: "LimitRange violation"},
			wantIssue: types.IssueLimitRangeViolation,
		},
		{
			name:      "5. ImagePullBackOff",
			ev:        types.DiagnosticEvent{PodName: "web-0", Reason: "ImagePullBackOff"},
			wantIssue: types.IssueImagePullBackOff,
		},
		{
			name:      "6. Sidecar Container Failure",
			ev:        types.DiagnosticEvent{Reason: "Sidecar startup failed"},
			wantIssue: types.IssueSidecarContainerFailure,
		},
		{
			name:      "7. Liveness Probe Failure",
			ev:        types.DiagnosticEvent{Reason: "Liveness probe unhealthy"},
			wantIssue: types.IssueLivenessProbeFailure,
		},
		{
			name:      "8. Pod Evicted DiskPressure",
			ev:        types.DiagnosticEvent{Reason: "Evicted DiskPressure"},
			wantIssue: types.IssuePodEvictedNodePressure,
		},
		{
			name:      "9. Pod Stuck Terminating",
			ev:        types.DiagnosticEvent{Reason: "Pod stuck terminating finalizer"},
			wantIssue: types.IssuePodStuckTerminating,
		},
		{
			name:      "10. HPA Maxed Out",
			ev:        types.DiagnosticEvent{HPAName: "api-hpa", Reason: "ScalingLimited maxReplicas"},
			wantIssue: types.IssueHPAMaxedOut,
		},
		{
			name:      "11. PDB Blocking Drain",
			ev:        types.DiagnosticEvent{PDBName: "db-pdb", Reason: "disruptionsAllowed=0"},
			wantIssue: types.IssuePDBBlockingDrain,
		},
		{
			name:      "12. DRA Allocation Failure",
			ev:        types.DiagnosticEvent{Reason: "ResourceClaim DRA allocation failed"},
			wantIssue: types.IssueDRAAllocationFailure,
		},
		{
			name:      "13. PVC Provisioning Pending",
			ev:        types.DiagnosticEvent{Reason: "ProvisioningFailed unbound immediate persistentvolumeclaims"},
			wantIssue: types.IssuePVCBindingPending,
		},
		{
			name:      "14. PVC Capacity Exhausted",
			ev:        types.DiagnosticEvent{PVCName: "data-0", CurrentStorage: "0", Reason: "PVCFilesystemFull"},
			wantIssue: types.IssuePVCCapacityExhausted,
		},
		{
			name:      "15. VolumeAttachment Multi-Attach",
			ev:        types.DiagnosticEvent{Reason: "Multi-Attach error for volumeattachment"},
			wantIssue: types.IssueVolumeAttachmentStuck,
		},
		{
			name:      "16. Service Endpoints Empty",
			ev:        types.DiagnosticEvent{ServiceName: "api-svc", Reason: "No endpoints available"},
			wantIssue: types.IssueServiceEndpointsEmpty,
		},
		{
			name:      "17. NetworkPolicy Drop",
			ev:        types.DiagnosticEvent{Reason: "NetworkPolicy OVN drop"},
			wantIssue: types.IssueNetworkPolicyDrop,
		},
		{
			name:      "18. OpenShift Route 503",
			ev:        types.DiagnosticEvent{RouteName: "app-route", Reason: "Route 503"},
			wantIssue: types.IssueOCPRouteDegraded,
		},
		{
			name:      "19. CoreDNS Failure",
			ev:        types.DiagnosticEvent{Reason: "CoreDNS SERVFAIL"},
			wantIssue: types.IssueCoreDNSFailure,
		},
		{
			name:      "20. Node NotReady",
			ev:        types.DiagnosticEvent{NodeName: "worker-1", Reason: "NodeNotReady PLEG"},
			wantIssue: types.IssueNodeNotReady,
		},
		{
			name:      "21. ClusterOperator Degraded",
			ev:        types.DiagnosticEvent{ClusterOperator: "ingress", Reason: "ClusterOperator Degraded"},
			wantIssue: types.IssueClusterOperatorDegrad,
		},
		{
			name:      "22. MachineConfigPool Degraded",
			ev:        types.DiagnosticEvent{MachineConfigPool: "worker", Reason: "MachineConfig drift"},
			wantIssue: types.IssueMCPNodeUpdatePaused,
		},
		{
			name:      "23. Etcd High Latency",
			ev:        types.DiagnosticEvent{Reason: "etcd fsync quorum slow"},
			wantIssue: types.IssueEtcdHighLatency,
		},
		{
			name:      "24. Certificate Expiring",
			ev:        types.DiagnosticEvent{Reason: "x509 CSR certificate expiring"},
			wantIssue: types.IssueCertificateExpiring,
		},
		{
			name:      "25. RBAC Forbidden",
			ev:        types.DiagnosticEvent{Reason: "RBAC forbidden SCC"},
			wantIssue: types.IssueRBACPermissionDenied,
		},
		{
			name:      "26. Default CrashLoopBackOff",
			ev:        types.DiagnosticEvent{PodName: "app-1", Reason: "CrashLoopBackOff"},
			wantIssue: types.IssueCrashLoopBackOff,
		},
		{
			name: "27a. Operator CR Adapter (within quota)",
			ev: types.DiagnosticEvent{
				OwnerAPIVersion:  "postgresql.cnpg.io/v1",
				OwnerKind:        "Cluster",
				OwnerName:        "pg-cluster",
				PodName:          "pg-cluster-1",
				ExitCode:         137,
				Reason:           "OOMKilled",
				CurrentMemoryLim: "256Mi",
			},
			wantIssue: types.IssuePodOOMKilled,
		},
		{
			name: "27b. Operator CR Adapter (exceeding quota)",
			ev: types.DiagnosticEvent{
				OwnerAPIVersion:  "postgresql.cnpg.io/v1",
				OwnerKind:        "Cluster",
				OwnerName:        "pg-cluster",
				ExitCode:         137,
				Reason:           "OOMKilled",
				CurrentMemoryLim: "512Mi",
				QuotaMemoryLimit: "512Mi",
				QuotaMemoryUsed:  "512Mi",
			},
			wantIssue: types.IssueNamespaceQuotaExceed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assess, err := eng.EvaluateAndAct(ctx, tc.ev)
			if err != nil {
				t.Fatalf("EvaluateAndAct failed: %v", err)
			}
			if assess.IssueType != tc.wantIssue {
				t.Fatalf("expected issueType %s, got %s", tc.wantIssue, assess.IssueType)
			}
		})
	}

	// Test ExecuteApprovedAction across all target kinds
	approvedActions := []types.RemediationAction{
		{
			TargetKind:      "ResourceQuota",
			TargetNamespace: "prod",
			TargetName:      "q1",
			PatchPayload:    `{}`,
		},
		{
			TargetKind:      "StatefulSet",
			TargetNamespace: "prod",
			TargetName:      "sts1",
			PatchPayload:    `{"spec":{"template":{"spec":{"containers":[{"name":"db","resources":{"requests":{"memory":"512Mi"},"limits":{"memory":"1Gi"}}}]}}}}`,
		},
		{
			TargetKind:   "MachineSet",
			TargetName:   "ms1",
			PatchPayload: `{}`,
		},
		{
			TargetAPIVersion: "postgresql.cnpg.io/v1",
			TargetKind:       "Cluster",
			TargetNamespace:  "prod",
			TargetName:       "pg-cluster",
			PatchPayload:     `{"spec":{"resources":{"limits":{"memory":"1Gi"}}}}`,
		},
	}
	for _, act := range approvedActions {
		if err := eng.ExecuteApprovedAction(ctx, act); err != nil {
			t.Fatalf("ExecuteApprovedAction(%s) failed: %v", act.TargetKind, err)
		}
	}
}
