package k8s

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/luigi/kube-doctor/internal/types"
)

func TestMemoryAndPluralizeHelpers(t *testing.T) {
	memCases := map[string]int64{
		"":           0,
		"2Gi":        2048,
		"1G":         953,
		"512Mi":      512,
		"256M":       256,
		"2048Ki":     2,
		"104857600":  100,
	}
	for in, want := range memCases {
		if got := ParseMemoryMiB(in); got != want {
			t.Fatalf("ParseMemoryMiB(%q) = %d, want %d", in, got, want)
		}
	}

	if FormatMemoryMiB(0) != "512Mi" || FormatMemoryMiB(2048) != "2Gi" || FormatMemoryMiB(768) != "768Mi" {
		t.Fatal("FormatMemoryMiB unexpected output")
	}

	kinds := []string{
		"Endpoints", "Ingress", "NetworkPolicy", "StorageClass", "BucketClass",
		"Prometheus", "NooBaa", "BackingStore", "NamespaceStore", "StorageCluster",
		"CephCluster", "Cluster", "Pooler", "Backup", "ScheduledBackup",
		"Pod", "StatefulSet", "Deployment", "Node", "Namespace",
	}
	for _, k := range kinds {
		if p := PluralizeKind(k); p == "" {
			t.Fatalf("PluralizeKind(%q) returned empty", k)
		}
		_ = inferDefaultAPIVersion(k)
		_ = BuildResourceListPath("v1", k, "prod")
		_ = BuildResourceListPath("apps/v1", k, "prod")
	}
}

func TestSimulatedClientAndHierarchy(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	c := NewClient()
	if !c.IsSimulated() {
		t.Fatal("expected simulated client")
	}

	ctx := context.Background()
	if err := c.GetJSON(ctx, "/api/v1/pods", & map[string]any{}); err == nil {
		t.Fatal("expected GetJSON error in simulation mode")
	}
	_ = c.DeletePod(ctx, "prod", "", "noop")
	_ = c.DeletePod(ctx, "prod", "pod-0", "delete")
	_, _ = c.PatchStatefulSetResources(ctx, "prod", "sts-1", "sts-1-0", "", "256Mi", "512Mi")
	_, _ = c.PatchWorkloadResources(ctx, "prod", "Deployment", "dep-1", "", "app", "256Mi", "512Mi")
	_, _ = c.PatchNamespaceResourceQuota(ctx, "prod", "", "4Gi", "4Gi")
	_, _ = c.ScaleOpenShiftMachineSet(ctx, "", 3)
	_ = c.CreateRemediationRequestCRD(ctx, "", "rem-1", `{}`)
	_ = c.PatchGenericResource(ctx, "v1", "Node", "", "node-1", `{}`, "patch node")
	if len(c.AuditEntries()) == 0 {
		t.Fatal("expected audit entries")
	}

	nodes, healthy, unhealthy, ver := c.GetClusterSummaryStats(ctx)
	if nodes != 1 || healthy != 8 || unhealthy != 0 || ver == "" {
		t.Fatalf("unexpected simulated stats: %d %d %d %s", nodes, healthy, unhealthy, ver)
	}
	if evs, err := c.ScanClusterHealth(ctx); err != nil || len(evs) != 0 {
		t.Fatalf("expected nil scan in simulation mode, got %v, %v", evs, err)
	}

	// Simulated hierarchy trees (NooBaa & CNPG)
	noobaaTree, err := c.ResolveOwnershipHierarchy(ctx, "", "", "NooBaa", "noobaa")
	if err != nil || noobaaTree.OperatorAdapter != "noobaa-odf" {
		t.Fatalf("expected noobaa-odf tree, got %+v (%v)", noobaaTree, err)
	}
	cnpgTree, err := c.ResolveOwnershipHierarchy(ctx, "prod", "", "Pod", "pg-cluster-1")
	if err != nil || cnpgTree.OperatorAdapter != "cloudnative-pg" {
		t.Fatalf("expected cloudnative-pg tree, got %+v (%v)", cnpgTree, err)
	}
}

func TestLiveClientScannerHierarchyAndCRDSchema(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"gitVersion": "v1.35.2"})
	})
	mux.HandleFunc("/api/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "worker-notready"},
					"status": map[string]any{
						"conditions": []map[string]string{
							{"type": "Ready", "status": "False", "reason": "KubeletNotReady", "message": "PLEG unhealthy"},
						},
					},
				},
			},
		})
	})
	mux.HandleFunc("/api/v1/persistentvolumeclaims", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "sys-pvc", "namespace": "kube-system"},
					"status":   map[string]any{"phase": "Pending"},
				},
				{
					"metadata": map[string]any{"name": "data-pvc-0", "namespace": "production"},
					"spec":     map[string]any{"resources": map[string]any{"requests": map[string]string{"storage": "20Gi"}}},
					"status":   map[string]any{"phase": "Pending"},
				},
			},
		})
	})
	mux.HandleFunc("/api/v1/pods", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "sys-pod", "namespace": "kube-system"},
					"status":   map[string]any{"phase": "Succeeded"},
				},
				{
					"metadata": map[string]any{
						"name":      "evicted-pod-0",
						"namespace": "production",
						"ownerReferences": []map[string]string{
							{"apiVersion": "apps/v1", "kind": "StatefulSet", "name": "evicted-sts"},
						},
					},
					"status": map[string]any{"phase": "Failed", "reason": "Evicted", "message": "DiskPressure"},
				},
				{
					"metadata": map[string]any{
						"name":      "oom-pod-0",
						"namespace": "production",
						"ownerReferences": []map[string]string{
							{"apiVersion": "apps/v1", "kind": "ReplicaSet", "name": "web-rs-123"},
						},
					},
					"spec": map[string]any{
						"containers": []map[string]any{
							{"name": "web", "resources": map[string]any{"limits": map[string]string{"memory": "128Mi"}, "requests": map[string]string{"memory": "64Mi"}}},
						},
					},
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []map[string]any{
							{
								"name":         "web",
								"ready":        false,
								"restartCount": 2,
								"lastState": map[string]any{
									"terminated": map[string]any{"exitCode": 137, "reason": "OOMKilled", "message": "cgroup OOM"},
								},
								"state": map[string]any{
									"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "back-off"},
								},
							},
						},
					},
				},
				{
					"metadata": map[string]any{
						"name":      "starved-oom-0",
						"namespace": "production",
						"ownerReferences": []map[string]string{
							{"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "agent-ds"},
						},
					},
					"spec": map[string]any{
						"containers": []map[string]any{
							{"name": "agent", "resources": map[string]any{"limits": map[string]string{"memory": "32Mi"}}},
						},
					},
					"status": map[string]any{
						"phase": "Pending",
						"containerStatuses": []map[string]any{
							{
								"name":  "agent",
								"ready": false,
								"state": map[string]any{
									"waiting": map[string]any{"reason": "RunContainerError", "message": "cannot allocate memory"},
								},
							},
						},
					},
				},
				{
					"metadata": map[string]any{"name": "imgpull-pod-0", "namespace": "production"},
					"status": map[string]any{
						"phase": "Pending",
						"containerStatuses": []map[string]any{
							{
								"name": "app",
								"state": map[string]any{
									"waiting": map[string]any{"reason": "ImagePullBackOff", "message": "pull failed"},
								},
							},
						},
					},
				},
				{
					"metadata": map[string]any{"name": "crash-pod-0", "namespace": "production"},
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []map[string]any{
							{
								"name":  "app",
								"ready": false,
								"state": map[string]any{
									"waiting": map[string]any{"reason": "CrashLoopBackOff", "message": "panic"},
								},
							},
						},
					},
				},
				{
					"metadata": map[string]any{"name": "term-pod-0", "namespace": "production"},
					"status": map[string]any{
						"phase": "Failed",
						"containerStatuses": []map[string]any{
							{
								"name":  "app",
								"ready": false,
								"state": map[string]any{
									"terminated": map[string]any{"exitCode": 2, "reason": ""},
								},
							},
						},
					},
				},
				{
					"metadata": map[string]any{"name": "healthy-pod-0", "namespace": "production"},
					"status": map[string]any{
						"phase": "Running",
						"containerStatuses": []map[string]any{
							{"name": "app", "ready": true},
						},
					},
				},
			},
		})
	})
	mux.HandleFunc("/apis/apps/v1/namespaces/production/replicasets/web-rs-123", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{
				"ownerReferences": []map[string]string{
					{"apiVersion": "apps/v1", "kind": "Deployment", "name": "web-dep"},
				},
			},
		})
	})
	mux.HandleFunc("/apis/apps/v1/namespaces/production/deployments/web-dep", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"spec": map[string]any{
				"template": map[string]any{
					"spec": map[string]any{
						"containers": []map[string]any{
							{"name": "web", "resources": map[string]any{"limits": map[string]string{"memory": "128Mi"}, "requests": map[string]string{"memory": "64Mi"}}},
						},
					},
				},
			},
		})
	})
	mux.HandleFunc("/api/v1/namespaces/production/resourcequotas", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "prod-quota", "namespace": "production"},
					"status": map[string]any{
						"hard": map[string]string{"limits.memory": "4Gi"},
						"used": map[string]string{"limits.memory": "1Gi"},
					},
				},
			},
		})
	})
	mux.HandleFunc("/apis/postgresql.cnpg.io/v1/clusters", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"apiVersion": "postgresql.cnpg.io/v1",
					"kind":       "Cluster",
					"metadata":   map[string]any{"name": "pg-cluster", "namespace": "production"},
					"spec": map[string]any{
						"instances": 3,
						"resources": map[string]any{
							"limits":   map[string]any{"memory": "256Mi"},
							"requests": map[string]any{"memory": "128Mi"},
						},
						"storage": map[string]any{"size": "10Gi"},
					},
					"status": map[string]any{
						"phase": "Failing",
						"conditions": []map[string]any{
							{"type": "Ready", "status": "False", "reason": "OOM", "message": "Pod OOMKilled 137"},
						},
					},
				},
				{
					"apiVersion": "postgresql.cnpg.io/v1",
					"kind":       "Cluster",
					"metadata":   map[string]any{"name": "pg-disk-cluster", "namespace": "production"},
					"spec":       map[string]any{"storage": map[string]any{"size": "10Gi"}},
					"status":     map[string]any{"phase": "Not enough disk space"},
				},
				{
					"apiVersion": "postgresql.cnpg.io/v1",
					"kind":       "Cluster",
					"metadata":   map[string]any{"name": "pg-deg-cluster", "namespace": "production"},
					"status":     map[string]any{"phase": "Degraded"},
				},
			},
		})
	})
	mux.HandleFunc("/apis/noobaa.io/v1alpha1/noobaas", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"apiVersion": "noobaa.io/v1alpha1",
					"kind":       "NooBaa",
					"metadata":   map[string]any{"name": "noobaa", "namespace": "openshift-storage"},
					"status":     map[string]any{"phase": "Rejected"},
				},
			},
		})
	})
	// Hierarchy endpoints
	mux.HandleFunc("/api/v1/namespaces/production/pods/pg-cluster-1", func(w http.ResponseWriter, r *http.Request) {
		controller := true
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":      "pg-cluster-1",
				"namespace": "production",
				"ownerReferences": []map[string]any{
					{"apiVersion": "postgresql.cnpg.io/v1", "kind": "Cluster", "name": "pg-cluster", "controller": controller},
				},
			},
			"spec": map[string]any{
				"containers": []map[string]any{
					{"name": "postgres", "resources": map[string]any{"limits": map[string]string{"memory": "256Mi"}}},
				},
			},
			"status": map[string]any{
				"phase": "Running",
				"containerStatuses": []map[string]any{
					{"name": "postgres", "ready": false, "state": map[string]any{"terminated": map[string]any{"exitCode": 137, "reason": "OOMKilled"}}},
				},
			},
		})
	})
	mux.HandleFunc("/api/v1/namespaces/production/pods/starved-oom-0", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"name": "starved-oom-0", "namespace": "production"},
			"spec": map[string]any{
				"containers": []map[string]any{
					{"name": "agent", "resources": map[string]any{"limits": map[string]string{"memory": "32Mi"}}},
				},
			},
			"status": map[string]any{
				"containerStatuses": []map[string]any{
					{"name": "agent", "ready": false, "state": map[string]any{"waiting": map[string]any{"reason": "RunContainerError"}}},
				},
			},
		})
	})
	mux.HandleFunc("/api/v1/namespaces/production/pods/pg-cluster", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/apis/postgresql.cnpg.io/v1/namespaces/production/clusters/pg-cluster", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "postgresql.cnpg.io/v1",
			"kind":       "Cluster",
			"metadata":   map[string]any{"name": "pg-cluster", "namespace": "production"},
			"spec": map[string]any{
				"instances":  3,
				"resources":  map[string]any{"limits": map[string]any{"memory": "256Mi"}, "requests": map[string]any{"memory": "128Mi"}},
				"storage":    map[string]any{"size": "10Gi"},
				"walStorage": map[string]any{"size": "5Gi"},
			},
			"status": map[string]any{
				"phase":          "Failing",
				"currentPrimary": "pg-cluster-1",
				"readyInstances": 2,
				"conditions": []map[string]any{
					{"type": "Ready", "status": "False", "reason": "OOMKilled", "message": "Pod OOM"},
				},
			},
		})
	})
	mux.HandleFunc("/apis/postgresql.cnpg.io/v1/namespaces/production/poolers", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "pg-cluster-pooler"},
					"spec":     map[string]any{"cluster": map[string]any{"name": "pg-cluster"}},
					"status":   map[string]any{"phase": "Healthy"},
				},
			},
		})
	})
	mux.HandleFunc("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/clusters.postgresql.cnpg.io", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"spec": map[string]any{
				"group": "postgresql.cnpg.io",
				"scope": "Namespaced",
				"versions": []map[string]any{
					{
						"name":    "v1",
						"served":  true,
						"storage": true,
						"schema": map[string]any{
							"openAPIV3Schema": map[string]any{
								"properties": map[string]any{
									"spec": map[string]any{
										"properties": map[string]any{
											"instances": map[string]any{"type": "integer", "description": "Number of instances"},
											"resources": map[string]any{"type": "object", "description": "Container resources"},
											"storage": map[string]any{
												"type": "object",
												"properties": map[string]any{
													"size": map[string]any{"type": "string", "description": "Storage size"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":[]}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	t.Setenv("KUBE_API_SERVER", srv.URL)
	t.Setenv("KUBE_BEARER_TOKEN", "live-token")
	t.Setenv("KUBE_SIMULATION_MODE", "false")

	c := NewClient()
	ctx := context.Background()

	// 1. GetClusterSummaryStats & ScanClusterHealth
	nodes, healthy, unhealthy, ver := c.GetClusterSummaryStats(ctx)
	if nodes != 1 || healthy < 1 || unhealthy < 1 || ver != "v1.35.2" {
		t.Fatalf("unexpected live stats: nodes=%d healthy=%d unhealthy=%d ver=%s", nodes, healthy, unhealthy, ver)
	}
	events, err := c.ScanClusterHealth(ctx)
	if err != nil || len(events) < 6 {
		t.Fatalf("expected >=6 diagnostic events from ScanClusterHealth, got %d (%v)", len(events), err)
	}

	// 2. EnrichDiagnosticEvent & PatchWorkloadResources on live mock cluster
	ev := types.DiagnosticEvent{Namespace: "production", PodName: "pg-cluster-1"}
	c.EnrichDiagnosticEvent(ctx, &ev)
	if ev.OwnerKind != "Cluster" || ev.QuotaName != "prod-quota" {
		t.Fatalf("EnrichDiagnosticEvent unexpected result: %+v", ev)
	}
	evStarved := types.DiagnosticEvent{Namespace: "production", PodName: "starved-oom-0"}
	c.EnrichDiagnosticEvent(ctx, &evStarved)
	if evStarved.Reason != "OOMKilled" || evStarved.ExitCode != 137 {
		t.Fatalf("EnrichDiagnosticEvent starved-oom-0 expected OOMKilled/137, got %+v", evStarved)
	}
	evOwnerOnly := types.DiagnosticEvent{Namespace: "production", OwnerKind: "Deployment", OwnerName: "web-dep"}
	c.EnrichDiagnosticEvent(ctx, &evOwnerOnly)
	if evOwnerOnly.CurrentMemoryLim != "128Mi" {
		t.Fatalf("EnrichDiagnosticEvent owner-only expected 128Mi, got %s", evOwnerOnly.CurrentMemoryLim)
	}

	if _, err := c.PatchWorkloadResources(ctx, "production", "Deployment", "web-dep", "oom-pod-0", "wrong-ctr", "128Mi", "256Mi"); err != nil {
		t.Fatalf("PatchWorkloadResources live failed: %v", err)
	}
	if _, err := c.PatchWorkloadResources(ctx, "production", "StatefulSet", "sts-1", "sts-1-0", "app", "128Mi", "256Mi"); err != nil {
		t.Fatalf("PatchWorkloadResources StatefulSet live failed: %v", err)
	}
	if err := c.CreateRemediationRequestCRD(ctx, "production", "rem-1", `{}`); err != nil {
		t.Fatalf("CreateRemediationRequestCRD live failed: %v", err)
	}

	// 3. InspectCRDSchemaAndInstance (live CRD schema + live CR introspection fallback)
	schema, err := c.InspectCRDSchemaAndInstance(ctx, "postgresql.cnpg.io/v1", "Cluster", "production", "pg-cluster")
	if err != nil || schema.SchemaSource != "live-cluster-crd" || len(schema.SpecFields) == 0 {
		t.Fatalf("InspectCRDSchemaAndInstance failed: %+v (%v)", schema, err)
	}

	// Test ResolveOwnershipHierarchy on live server (pg-cluster-1 -> pg-cluster + related Pooler)
	liveTree, err := c.ResolveOwnershipHierarchy(ctx, "production", "v1", "Pod", "pg-cluster-1")
	if err != nil || liveTree.OperatorAdapter != "cloudnative-pg" || len(liveTree.OwnershipChain) != 2 || len(liveTree.RelatedCRs) == 0 {
		t.Fatalf("ResolveOwnershipHierarchy live failed: %+v (%v)", liveTree, err)
	}
	// Test Pod -> Cluster fallback at depth 0 when Pod is not found
	fallbackTree, err := c.ResolveOwnershipHierarchy(ctx, "production", "", "Pod", "pg-cluster")
	if err != nil || fallbackTree.RootController == nil || fallbackTree.RootController.Kind != "Cluster" {
		t.Fatalf("ResolveOwnershipHierarchy fallback failed: %+v (%v)", fallbackTree, err)
	}

	// Test PatchNamespaceResourceQuota, ScaleOpenShiftMachineSet, PatchGenericResource, DeletePod on live server
	if _, err := c.PatchNamespaceResourceQuota(ctx, "production", "prod-quota", "8Gi", "4Gi"); err != nil {
		t.Fatalf("PatchNamespaceResourceQuota live failed: %v", err)
	}
	if _, err := c.ScaleOpenShiftMachineSet(ctx, "worker-ms", 3); err != nil {
		t.Fatalf("ScaleOpenShiftMachineSet live failed: %v", err)
	}
	if err := c.PatchGenericResource(ctx, "postgresql.cnpg.io/v1", "Cluster", "production", "pg-cluster", `{"spec":{"instances":3}}`, "patch"); err != nil {
		t.Fatalf("PatchGenericResource live failed: %v", err)
	}
	if err := c.DeletePod(ctx, "production", "pg-cluster-1", "recycle"); err != nil {
		t.Fatalf("DeletePod live failed: %v", err)
	}

	// Test helper functions directly for full branch coverage
	rawFields := extractFieldsFromLiveSpecMap("spec", map[string]any{
		"replicas": 3,
		"enabled":  true,
		"image":    "postgres:16",
		"nested":   map[string]any{"size": "10Gi", "count": float64(2)},
	})
	if len(rawFields) != 5 {
		t.Fatalf("expected 5 fields from extractFieldsFromLiveSpecMap, got %d", len(rawFields))
	}

	var openAPIFields []CRDSchemaField
	walkOpenAPINode("spec", map[string]any{
		"properties": map[string]any{
			"limits": map[string]any{"type": "object", "description": strings.Repeat("x", 180)},
			"plain":  map[string]any{"description": "no type"},
		},
	}, 0, &openAPIFields)
	if len(openAPIFields) < 3 {
		t.Fatalf("expected >=3 fields from walkOpenAPINode, got %d", len(openAPIFields))
	}

	for _, tc := range []struct {
		av, kind, want string
	}{
		{"kafka.strimzi.io/v1beta2", "Kafka", "strimzi-kafka"},
		{"monitoring.coreos.com/v1", "Prometheus", "prometheus-operator"},
		{"argoproj.io/v1alpha1", "Application", "argocd-operator"},
		{"noobaa.io/v1alpha1", "BackingStore", "noobaa-odf"},
		{"apps/v1", "Deployment", ""},
	} {
		got := detectOperatorAdapterFromChain([]types.OwnerChainLink{{APIVersion: tc.av, Kind: tc.kind}})
		if got != tc.want {
			t.Fatalf("detectOperatorAdapterFromChain(%s, %s) = %q, want %q", tc.av, tc.kind, got, tc.want)
		}
	}

	for _, k := range []string{"Pod", "StatefulSet", "Cluster", "NooBaa", "StorageCluster", "Kafka", "Unknown"} {
		if inferDefaultAPIVersion(k) == "" {
			t.Fatalf("inferDefaultAPIVersion(%s) returned empty", k)
		}
	}
	if extractStatusPhase(map[string]any{"state": "Ready"}) != "Ready" || extractStatusPhase(nil) != "" {
		t.Fatalf("extractStatusPhase state fallback failed")
	}
	kf := extractKeySpecFields("NooBaa", map[string]any{
		"replicas":            2,
		"dbResources":         map[string]any{"limits": map[string]any{"memory": "4Gi"}},
		"coreResources":       map[string]any{"limits": map[string]any{"memory": "4Gi"}},
		"defaultBackingStore": "bs-1",
	}, nil)
	if kf["dbMemoryLimit"] != "4Gi" || kf["coreMemoryLimit"] != "4Gi" || kf["defaultBackingStore"] != "bs-1" {
		t.Fatalf("extractKeySpecFields NooBaa failed: %+v", kf)
	}

	// Test label-based lineage and NooBaa related CR discovery on a mock server
	nbMux := http.NewServeMux()
	nbMux.HandleFunc("/api/v1/namespaces/openshift-storage/pods/noobaa-core-0", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"name":      "noobaa-core-0",
				"namespace": "openshift-storage",
				"labels":    map[string]string{"noobaa-core": "noobaa"},
			},
		})
	})
	nbMux.HandleFunc("/apis/noobaa.io/v1alpha1/namespaces/openshift-storage/noobaas/noobaa", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"apiVersion": "noobaa.io/v1alpha1",
			"kind":       "NooBaa",
			"metadata": map[string]any{
				"name":      "noobaa",
				"namespace": "openshift-storage",
				"ownerReferences": []map[string]any{
					{"apiVersion": "ocs.openshift.io/v1", "kind": "StorageCluster", "name": "ocs-storagecluster"},
				},
			},
			"spec": map[string]any{"coreResources": map[string]any{"limits": map[string]any{"memory": "4Gi"}}},
		})
	})
	nbMux.HandleFunc("/apis/noobaa.io/v1alpha1/namespaces/openshift-storage/backingstores", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"metadata": map[string]any{"name": "noobaa-pv-bs"}, "status": map[string]any{"phase": "Rejected"}},
			},
		})
	})
	nbMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	nbSrv := httptest.NewServer(nbMux)
	defer nbSrv.Close()
	t.Setenv("KUBE_API_SERVER", nbSrv.URL)
	nbClient := NewClient()
	nbTree, err := nbClient.ResolveOwnershipHierarchy(ctx, "openshift-storage", "", "Pod", "noobaa-core-0")
	if err != nil || nbTree.OperatorAdapter != "noobaa-odf" || len(nbTree.RelatedCRs) == 0 {
		t.Fatalf("expected noobaa-odf tree with related CRs, got %+v (%v)", nbTree, err)
	}
	// Also test InspectCRDSchemaAndInstance fallback to live-cr-introspection when CRD endpoint returns 404
	nbSchema, err := nbClient.InspectCRDSchemaAndInstance(ctx, "noobaa.io/v1alpha1", "NooBaa", "openshift-storage", "noobaa")
	if err != nil || nbSchema.SchemaSource != "live-cr-introspection" {
		t.Fatalf("expected live-cr-introspection fallback, got %+v (%v)", nbSchema, err)
	}

	// Test doPatch/doPost/GetJSON non-2xx error handling and resolveOwner/populateQuota branches
	errMux := http.NewServeMux()
	errMux.HandleFunc("/api/v1/namespaces/ns/pods/cnpg-lbl-pod", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"name": "cnpg-lbl-pod", "namespace": "ns", "labels": map[string]string{"cnpg.io/cluster": "my-pg"}},
		})
	})
	errMux.HandleFunc("/api/v1/namespaces/ns/pods/strimzi-lbl-pod", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"metadata": map[string]any{"name": "strimzi-lbl-pod", "namespace": "ns", "labels": map[string]string{"strimzi.io/cluster": "my-kafka"}},
		})
	})
	errMux.HandleFunc("/api/v1/namespaces/ns/resourcequotas", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{
					"metadata": map[string]any{"name": "mem-quota"},
					"status":   map[string]any{"hard": map[string]string{"memory": "2Gi"}, "used": map[string]string{"memory": "512Mi"}},
				},
			},
		})
	})
	errMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	})
	errSrv := httptest.NewServer(errMux)
	defer errSrv.Close()
	t.Setenv("KUBE_API_SERVER", errSrv.URL)
	errClient := NewClient()
	_, _ = errClient.ResolveOwnershipHierarchy(ctx, "ns", "v1", "Pod", "cnpg-lbl-pod")
	_, _ = errClient.ResolveOwnershipHierarchy(ctx, "ns", "v1", "Pod", "strimzi-lbl-pod")
	qEv := types.DiagnosticEvent{Namespace: "ns", OwnerKind: "Deployment", OwnerName: "dep-1"}
	errClient.EnrichDiagnosticEvent(ctx, &qEv)
	if qEv.QuotaMemoryLimit != "2Gi" {
		t.Fatalf("expected QuotaMemoryLimit=2Gi from memory fallback, got %s", qEv.QuotaMemoryLimit)
	}
	if err := errClient.PatchGenericResource(ctx, "v1", "Pod", "ns", "p", `{}`, "fail"); err == nil {
		t.Fatalf("expected error from 500 PATCH")
	}
	if err := errClient.CreateRemediationRequestCRD(ctx, "", "rem-err", `{}`); err == nil {
		t.Fatalf("expected error from 500 POST")
	}
	ownerK, ownerN := errClient.resolveOwner(ctx, "ns", []k8sOwnerRef{{Kind: "StatefulSet", Name: "sts-x"}}, "sts-x-0")
	if ownerK != "StatefulSet" || ownerN != "sts-x" {
		t.Fatalf("resolveOwner StatefulSet failed: %s/%s", ownerK, ownerN)
	}
	ownerK2, ownerN2 := errClient.resolveOwner(ctx, "ns", []k8sOwnerRef{{Kind: "ReplicaSet", Name: "dep-abc"}}, "dep-abc-1")
	if ownerK2 != "Deployment" || ownerN2 != "dep" {
		t.Fatalf("resolveOwner ReplicaSet fallback failed: %s/%s", ownerK2, ownerN2)
	}
	ownerK3, _ := errClient.resolveOwner(ctx, "ns", []k8sOwnerRef{{Kind: "Job", Name: "job-1"}}, "job-1-x")
	if ownerK3 != "Job" {
		t.Fatalf("resolveOwner custom kind failed: %s", ownerK3)
	}

	// Test KUBERNETES_SERVICE_HOST / KUBERNETES_SERVICE_PORT env fallback
	t.Setenv("KUBE_API_SERVER", "")
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	t.Setenv("KUBERNETES_SERVICE_PORT", "443")
	inClusterClient := NewClient()
	if inClusterClient.IsSimulated() {
		t.Fatalf("expected inClusterClient with KUBERNETES_SERVICE_HOST to not be simulated")
	}

	// Test loadFromKubeconfig with a generated X509 cert/key pair
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

	tmpKC := filepath.Join(t.TempDir(), "kubeconfig")
	kcContent := "apiVersion: v1\nclusters:\n- cluster:\n    certificate-authority-data: " + base64.StdEncoding.EncodeToString(certPEM) + "\n    server: https://127.0.0.1:6443\nusers:\n- user:\n    client-certificate-data: " + base64.StdEncoding.EncodeToString(certPEM) + "\n    client-key-data: " + base64.StdEncoding.EncodeToString(keyPEM) + "\n    token: my-kc-token\n"
	_ = os.WriteFile(tmpKC, []byte(kcContent), 0600)
	t.Setenv("KUBECONFIG", tmpKC+":/other/path")
	kcSrv, kcTok, kcCert, kcCA, ok := loadFromKubeconfig()
	if !ok || kcSrv != "https://127.0.0.1:6443" || kcTok != "my-kc-token" || kcCert == nil || len(kcCA) == 0 {
		t.Fatalf("loadFromKubeconfig failed: ok=%v srv=%s tok=%s", ok, kcSrv, kcTok)
	}
}
