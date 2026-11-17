package watcher

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

type mockLLMDiagnostician struct{}

func (m *mockLLMDiagnostician) DiagnoseAndRemediateEvent(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, string, error) {
	return &types.TriageAssessment{
		EventID:     ev.ID,
		IssueType:   types.IssuePodOOMKilled,
		Criticality: types.CriticalityP1High,
		Severity:    types.SeveritySev2,
		Summary:     "Mock diagnosed " + ev.Reason,
		PlannedActions: []types.RemediationAction{
			{
				TargetAPIVersion: "postgresql.cnpg.io/v1",
				TargetKind:       "Cluster",
				TargetNamespace:  ev.Namespace,
				TargetName:       "pg-cluster",
				Status:           "EXECUTED",
			},
		},
	}, "Mock LLM analysis", nil
}

func TestWatcherScanAlertmanagerAndEffectiveness(t *testing.T) {
	// Mock Kubernetes API server returning an OOMKilled pod, a healthy pod, and a CNPG Cluster CR
	mux := http.NewServeMux()
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"gitVersion": "v1.35.2"})
	})
	mux.HandleFunc("/api/v1/pods", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "db-0", "namespace": "production"},
					"spec": map[string]any{
						"nodeName": "worker-1",
						"containers": []map[string]any{
							{"name": "db", "resources": map[string]any{"limits": map[string]string{"memory": "256Mi"}}},
						},
					},
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []map[string]any{
							{
								"name":         "db",
								"ready":        false,
								"restartCount": 3,
								"state": map[string]any{
									"terminated": map[string]any{"exitCode": 137, "reason": "OOMKilled"},
								},
							},
						},
					},
				},
			},
		})
	})
	mux.HandleFunc("/api/v1/namespaces/production/pods/db-0", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": map[string]any{
				"phase": "Running",
				"containerStatuses": []map[string]any{
					{"name": "db", "ready": true, "restartCount": 0},
				},
			},
		})
	})
	mux.HandleFunc("/apis/postgresql.cnpg.io/v1/namespaces/production/clusters/pg-cluster", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"spec":   map[string]any{"instances": 3},
			"status": map[string]any{"phase": "Cluster in healthy state"},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}})
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	t.Setenv("KUBE_API_SERVER", srv.URL)
	t.Setenv("KUBE_BEARER_TOKEN", "tok")
	t.Setenv("KUBE_SIMULATION_MODE", "false")
	t.Setenv("WATCH_INTERVAL_SECONDS", "5")
	t.Setenv("KUBE_DOCTOR_CLUSTER_ID", "test-cluster")
	t.Setenv("KUBE_DOCTOR_MODE", "agent")

	kc := k8s.NewClient()
	gm := gitops.NewManager(kc)
	rem := remediation.NewEngine(kc, gm)
	w := NewWatcher(kc, rem, &mockLLMDiagnostician{})

	ctx := context.Background()

	// 1. ScanAndRemediate on live mock cluster
	recs, err := w.ScanAndRemediate(ctx, "CLUSTER_WATCHER", false)
	if err != nil || len(recs) == 0 {
		t.Fatalf("ScanAndRemediate failed: %v, recs=%d", err, len(recs))
	}
	// Second scan within 5m cooldown skips already-seen events unless forced
	recsCooldown, _ := w.ScanAndRemediate(ctx, "CLUSTER_WATCHER", false)
	if len(recsCooldown) != 0 {
		t.Fatalf("expected 0 records during cooldown, got %d", len(recsCooldown))
	}
	// Forced scan re-evaluates
	recsForced, _ := w.ScanAndRemediate(ctx, "CLUSTER_WATCHER", true)
	if len(recsForced) == 0 {
		t.Fatal("expected forced scan to re-evaluate")
	}

	// 2. HandleAlertmanagerWebhook across all alert types (oom, crashloop, quota, nodenotready, pvc full, pvc pending, machine, resolved)
	alerts := AlertmanagerPayload{
		Version: "4",
		Status:  "firing",
		Alerts: []AlertmanagerAlert{
			{Status: "resolved", Labels: map[string]string{"alertname": "ResolvedAlert"}},
			{Status: "firing", Labels: map[string]string{"alertname": "KubePodOOMKilled", "namespace": "production", "pod": "db-0", "statefulset": "db"}},
			{Status: "firing", Labels: map[string]string{"alertname": "KubePodCrashLooping", "namespace": "production", "deployment": "api"}},
			{Status: "firing", Labels: map[string]string{"alertname": "KubeQuotaExceeded", "namespace": "production"}},
			{Status: "firing", Labels: map[string]string{"alertname": "KubeNodeNotReady", "node": "worker-1"}},
			{Status: "firing", Labels: map[string]string{"alertname": "KubePersistentVolumeFillingUp", "persistentvolumeclaim": "data-0"}},
			{Status: "firing", Labels: map[string]string{"alertname": "KubePersistentVolumePending", "persistentvolumeclaim": "data-1"}},
			{Status: "firing", Labels: map[string]string{"alertname": "MachineMissing", "machineset": "ms-1"}},
		},
	}
	wNoLLM := NewWatcher(kc, rem)
	whRecs, err := wNoLLM.HandleAlertmanagerWebhook(ctx, alerts)
	if err != nil || len(whRecs) != 7 {
		t.Fatalf("HandleAlertmanagerWebhook expected 7 records, got %d (%v)", len(whRecs), err)
	}

	// 3. ListIncidents, ListActiveIncidents, ListResolvedIncidents, VerifyRemediationEffectiveness, Status
	_ = wNoLLM.VerifyRemediationEffectiveness(ctx)
	if len(wNoLLM.ListIncidents()) == 0 {
		t.Fatal("expected incidents")
	}
	_ = wNoLLM.ListActiveIncidents()
	_ = wNoLLM.ListResolvedIncidents()
	st := wNoLLM.Status()
	if st["totalIncidents"].(int) == 0 {
		t.Fatalf("unexpected watcher status: %+v", st)
	}

	// Test Start with DISABLE_CLUSTER_WATCHER=true and false
	t.Setenv("DISABLE_CLUSTER_WATCHER", "true")
	wNoLLM.Start(ctx)
	t.Setenv("DISABLE_CLUSTER_WATCHER", "false")
	cCtx, cancel := context.WithCancel(ctx)
	wNoLLM.Start(cCtx)
	cancel()
}
