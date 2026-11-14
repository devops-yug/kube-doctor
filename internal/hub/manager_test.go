package hub

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
	"github.com/luigi/kube-doctor/internal/watcher"
)

func TestHubAndSpokeAgentSync(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	t.Setenv("DISABLE_UPSTREAM_LLM", "true")
	t.Setenv("KUBE_DOCTOR_CLUSTER_ID", "hub-primary")

	kc := k8s.NewClient()
	gm := gitops.NewManager(kc)
	rem := remediation.NewEngine(kc, gm)
	w := watcher.NewWatcher(kc, rem)
	hm := NewManager("hub", kc, rem, w)

	if hm.Mode() != ModeHub || hm.LocalClusterID() != "hub-primary" {
		t.Fatalf("unexpected mode/clusterId: %s / %s", hm.Mode(), hm.LocalClusterID())
	}

	ctx := context.Background()

	// 1. RegisterOrHeartbeat validation & spoke registration with events + incidents
	if _, _, err := hm.RegisterOrHeartbeat(ctx, HeartbeatPayload{}); err == nil {
		t.Fatal("expected error when clusterId is empty")
	}

	spokeCluster, newIncs, err := hm.RegisterOrHeartbeat(ctx, HeartbeatPayload{
		ClusterID:         "spoke-edge-1",
		DisplayName:       "Spoke Edge 1",
		Environment:       "edge",
		Region:            "us-east",
		KubernetesVersion: "v1.35.2",
		OpenShiftVersion:  "4.22.1",
		AgentEndpoint:     "http://10.0.0.5:8080",
		NodeCount:         3,
		HealthyPodCount:   20,
		UnhealthyPodCount: 1,
		InstalledAdapters: []string{"cloudnative-pg"},
		Events: []types.DiagnosticEvent{
			{
				ID:               "ev-spoke-1",
				Namespace:        "prod",
				PodName:          "db-0",
				OwnerKind:        "StatefulSet",
				OwnerName:        "db",
				ExitCode:         137,
				Reason:           "OOMKilled",
				CurrentMemoryLim: "256Mi",
			},
		},
		Incidents: []watcher.IncidentRecord{
			{
				IncidentID: "inc-spoke-resolved-1",
				Status:     "RESOLVED",
				Event:      types.DiagnosticEvent{Namespace: "prod", NodeName: "edge-node-1"},
			},
		},
	})
	if err != nil || spokeCluster.ClusterID != "spoke-edge-1" || len(newIncs) != 1 {
		t.Fatalf("RegisterOrHeartbeat failed: %+v, newIncs=%d, err=%v", spokeCluster, len(newIncs), err)
	}

	// Duplicate event is deduplicated on next heartbeat
	_, dupIncs, _ := hm.RegisterOrHeartbeat(ctx, HeartbeatPayload{
		ClusterID: "spoke-edge-1",
		Events: []types.DiagnosticEvent{
			{ID: "ev-spoke-1", Namespace: "prod", PodName: "db-0", Reason: "OOMKilled"},
		},
		Incidents: []watcher.IncidentRecord{
			{IncidentID: "inc-spoke-resolved-1", Status: "ACTIVE"},
		},
	})
	if len(dupIncs) != 0 {
		t.Fatalf("expected 0 duplicate incidents, got %d", len(dupIncs))
	}

	// 2. Check PendingCommands, DispatchCommand (local & remote), AckCommand
	pending := hm.PendingCommands("spoke-edge-1")
	if len(pending) == 0 {
		t.Fatal("expected pending auto-remediate command for spoke-edge-1")
	}
	if _, err := hm.AckCommand("spoke-edge-1", pending[0].CommandID, "", "done"); err != nil {
		t.Fatalf("AckCommand failed: %v", err)
	}
	if _, err := hm.AckCommand("spoke-edge-1", "unknown-cmd", "EXECUTED", ""); err == nil {
		t.Fatal("expected error for unknown command ack")
	}

	// Local cluster dispatch executes immediately
	localCmd, err := hm.DispatchCommand(ctx, "", types.RemediationAction{
		TargetKind:      "StatefulSet",
		TargetNamespace: "prod",
		TargetName:      "db",
		PatchPayload:    `{}`,
	})
	if err != nil || localCmd.Status != "EXECUTED" {
		t.Fatalf("expected local command EXECUTED, got %+v (%v)", localCmd, err)
	}

	// 3. ListClusters, ListFleetIncidents, ListActiveFleetIncidents, ListResolvedFleetIncidents
	if len(hm.ListClusters()) < 2 {
		t.Fatal("expected at least 2 clusters")
	}
	if len(hm.ListFleetIncidents("")) == 0 || len(hm.ListFleetIncidents("spoke-edge-1")) == 0 {
		t.Fatal("expected fleet incidents")
	}
	_ = hm.ListActiveFleetIncidents("")
	_ = hm.ListResolvedFleetIncidents("")
	_ = hasUnresolvedIncident(hm.ListFleetIncidents("spoke-edge-1"))

	// 4. Test Spoke Agent -> Hub HTTP sync loop (syncAgentWithHub)
	hubMux := http.NewServeMux()
	hubMux.HandleFunc("/v1/hub/clusters/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	hubMux.HandleFunc("/v1/hub/clusters/spoke-pi/commands", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"commands": []AgentCommand{
				{
					CommandID: "cmd-pi-1",
					ClusterID: "spoke-pi",
					Action: types.RemediationAction{
						TargetKind:      "StatefulSet",
						TargetNamespace: "default",
						TargetName:      "pi-app",
						Operation:       "PATCH",
						PatchPayload:    `{}`,
					},
				},
			},
		})
	})
	hubMux.HandleFunc("/v1/hub/clusters/spoke-pi/commands/cmd-pi-1/ack", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	hubSrv := httptest.NewServer(hubMux)
	defer hubSrv.Close()

	t.Setenv("KUBE_DOCTOR_CLUSTER_ID", "spoke-pi")
	t.Setenv("KUBE_DOCTOR_HUB_URL", hubSrv.URL)
	agentMgr := NewManager("agent", kc, rem, w)
	sCtx, cancel := context.WithCancel(ctx)
	agentMgr.Start(sCtx)
	agentMgr.syncAgentWithHub(ctx)
	cancel()
}
