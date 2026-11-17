package operator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
)

func TestOperatorControllerReconcile(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/apis/kubedoctor.io/v1alpha1/kubedoctors", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"apiVersion": "kubedoctor.io/v1alpha1",
					"kind":       "KubeDoctor",
					"metadata": map[string]any{
						"name":      "kube-doctor-cluster",
						"namespace": "kube-doctor-system",
					},
					"spec": map[string]any{
						"mode":              "standalone",
						"approvalMode":      "IN_CLUSTER",
						"inClusterApproval": true,
						"consolePlugin":     map[string]any{"enabled": true},
						"gitops": map[string]any{
							"github": map[string]any{"clientId": "gh-op-id", "repository": "org/repo"},
							"gitlab": map[string]any{"clientId": "gl-op-id", "repository": "org/repo"},
						},
					},
				},
			},
		})
	})
	mux.HandleFunc("/apis/kubedoctor.io/v1alpha1/remediationrequests", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"apiVersion": "kubedoctor.io/v1alpha1",
					"kind":       "RemediationRequest",
					"metadata": map[string]any{
						"name":      "remediate-direct-crd-1",
						"namespace": "production",
						"annotations": map[string]string{
							"kubedoctor.io/approved-by": "ocp-admin",
						},
					},
					"spec": map[string]any{
						"approved": true,
						"targetResource": map[string]any{
							"apiVersion": "v1",
							"kind":       "ResourceQuota",
							"namespace":  "production",
							"name":       "compute-quota",
						},
						"operation":    "UPDATE_NAMESPACE_RESOURCE_QUOTA",
						"patchPayload": `{"spec":{"hard":{"limits.memory":"8Gi","requests.memory":"8Gi"}}}`,
					},
					"status": map[string]any{
						"phase": "PendingApproval",
					},
				},
				{
					"apiVersion": "kubedoctor.io/v1alpha1",
					"kind":       "RemediationRequest",
					"metadata":   map[string]any{"name": "already-executed", "namespace": "production"},
					"spec":       map[string]any{"approved": true},
					"status":     map[string]any{"phase": "Executed"},
				},
				{
					"apiVersion": "kubedoctor.io/v1alpha1",
					"kind":       "RemediationRequest",
					"metadata":   map[string]any{"name": "still-pending", "namespace": "production"},
					"spec":       map[string]any{"approved": false},
					"status":     map[string]any{"phase": "PendingApproval"},
				},
			},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	t.Setenv("KUBE_API_SERVER", srv.URL)
	t.Setenv("KUBE_BEARER_TOKEN", "test-token")
	t.Setenv("KUBE_SIMULATION_MODE", "false")

	kc := k8s.NewClient()
	gm := gitops.NewManager(kc)
	rem := remediation.NewEngine(kc, gm)
	ctrl := NewController(kc, gm, rem)

	// Also add a pending approval in gitops.Manager so pendingApprovals count is exercised
	_, _ = gm.CreateApprovalRequest(context.Background(), types.IssueNamespaceQuotaExceed, types.CriticalityP0Critical, types.SeveritySev1, "test", types.RemediationAction{TargetKind: "ResourceQuota", TargetName: "q1", TargetNamespace: "production"}, nil)

	summary, err := ctrl.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("ReconcileOnce failed: %v", err)
	}
	if summary.KubeDoctorCRsReconciled != 1 || summary.RemediationCRsReconciled != 3 || summary.InClusterApprovalsApplied != 1 {
		t.Fatalf("unexpected reconcile summary: %+v", summary)
	}
	if ctrl.Status().OperatorPhase != "Succeeded" {
		t.Fatalf("unexpected operator phase: %s", ctrl.Status().OperatorPhase)
	}

	ctx, cancel := context.WithCancel(context.Background())
	ctrl.Start(ctx)
	cancel()
}
