package adapters_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/luigi/kube-doctor/internal/adapters"
	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/hub"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
	"github.com/luigi/kube-doctor/internal/watcher"
)

func TestCloudNativePGAdapterAndHierarchy(t *testing.T) {
	ctx := context.Background()
	k8sClient := k8s.NewClient()
	gitopsMgr := gitops.NewManager(k8sClient)
	remEngine := remediation.NewEngine(k8sClient, gitopsMgr)

	// 1. Verify recursive ownership hierarchy resolution for pg-cluster-1
	tree, err := k8sClient.ResolveOwnershipHierarchy(ctx, "default", "v1", "Pod", "pg-cluster-1")
	if err != nil {
		t.Fatalf("ResolveOwnershipHierarchy failed: %v", err)
	}
	if tree.OperatorAdapter != "cloudnative-pg" {
		t.Fatalf("expected OperatorAdapter=cloudnative-pg, got %q", tree.OperatorAdapter)
	}
	if tree.RootController == nil || tree.RootController.Kind != "Cluster" || tree.RootController.Name != "pg-cluster" {
		t.Fatalf("expected RootController Cluster/pg-cluster, got %+v", tree.RootController)
	}

	// 2. Verify Remediation Engine routes pg-cluster OOMKilled to CloudNativePG Adapter (patching Cluster.postgresql.cnpg.io, NOT StatefulSet)
	ev := types.DiagnosticEvent{
		Namespace:        "default",
		PodName:          "pg-cluster-1",
		OwnerAPIVersion:  "postgresql.cnpg.io/v1",
		OwnerKind:        "Cluster",
		OwnerName:        "pg-cluster",
		ExitCode:         137,
		Reason:           "OOMKilled",
		CurrentMemoryLim: "256Mi",
		CurrentMemoryReq: "128Mi",
		QuotaMemoryLimit: "4Gi",
		QuotaMemoryUsed:  "512Mi",
	}
	assessment, err := remEngine.EvaluateAndAct(ctx, ev)
	if err != nil {
		t.Fatalf("EvaluateAndAct failed: %v", err)
	}
	if assessment.OperatorAdapter != "cloudnative-pg" {
		t.Fatalf("expected assessment.OperatorAdapter=cloudnative-pg, got %q", assessment.OperatorAdapter)
	}
	if len(assessment.PlannedActions) == 0 {
		t.Fatalf("expected planned action for pg-cluster")
	}
	act := assessment.PlannedActions[0]
	if act.TargetAPIVersion != "postgresql.cnpg.io/v1" || act.TargetKind != "Cluster" || act.TargetName != "pg-cluster" {
		t.Fatalf("expected target postgresql.cnpg.io/v1 Cluster/pg-cluster, got %s %s/%s", act.TargetAPIVersion, act.TargetKind, act.TargetName)
	}
	if !strings.Contains(act.PatchPayload, `"512Mi"`) {
		t.Fatalf("expected patched memory limit 512Mi in payload: %s", act.PatchPayload)
	}
}

func TestNooBaa5CRHierarchyAndMultiClusterHub(t *testing.T) {
	ctx := context.Background()
	k8sClient := k8s.NewClient()
	gitopsMgr := gitops.NewManager(k8sClient)
	remEngine := remediation.NewEngine(k8sClient, gitopsMgr)
	w := watcher.NewWatcher(k8sClient, remEngine)

	// 1. Verify 5-CR NooBaa/ODF hierarchy resolution
	tree, err := k8sClient.ResolveOwnershipHierarchy(ctx, "openshift-storage", "v1", "Pod", "noobaa-core-0")
	if err != nil {
		t.Fatalf("ResolveOwnershipHierarchy failed: %v", err)
	}
	if tree.OperatorAdapter != "noobaa-odf" {
		t.Fatalf("expected noobaa-odf adapter, got %q", tree.OperatorAdapter)
	}
	if len(tree.OwnershipChain) < 4 || len(tree.RelatedCRs) < 3 {
		t.Fatalf("expected multi-hop chain + sibling CRs for NooBaa 5-CR hierarchy, got chain=%d related=%d", len(tree.OwnershipChain), len(tree.RelatedCRs))
	}

	// 2. Verify Multi-Cluster Hub Spoke Registration, Event Triage, and Command Queue
	hubMgr := hub.NewManager("hub", k8sClient, remEngine, w)
	cluster, newIncs, err := hubMgr.RegisterOrHeartbeat(ctx, hub.HeartbeatPayload{
		ClusterID:         "ocp-prod-us-east",
		DisplayName:       "OpenShift 4.22 Production US-East",
		Environment:       "production",
		Region:            "us-east-1",
		KubernetesVersion: "v1.35.2",
		OpenShiftVersion:  "4.22.1",
		NodeCount:         12,
		HealthyPodCount:   140,
		UnhealthyPodCount: 1,
		Events: []types.DiagnosticEvent{
			{
				Namespace:        "openshift-storage",
				PodName:          "noobaa-core-0",
				OwnerAPIVersion:  "noobaa.io/v1alpha1",
				OwnerKind:        "NooBaa",
				OwnerName:        "noobaa",
				OperatorAdapter:  "noobaa-odf",
				OwnershipChain:   tree.OwnershipChain,
				Reason:           "OperatorCRDegraded",
				Message:          "NooBaa BackingStore noobaa-pv-backing-store in Rejected phase",
				CurrentMemoryLim: "2Gi",
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterOrHeartbeat failed: %v", err)
	}
	if cluster.ClusterID != "ocp-prod-us-east" || len(newIncs) != 1 {
		t.Fatalf("expected 1 new incident for spoke ocp-prod-us-east, got %d", len(newIncs))
	}
	cmds := hubMgr.PendingCommands("ocp-prod-us-east")
	if len(cmds) != 1 || cmds[0].Action.TargetKind != "NooBaa" {
		t.Fatalf("expected 1 queued AgentCommand targeting NooBaa CR for ocp-prod-us-east, got %+v", cmds)
	}
	acked, err := hubMgr.AckCommand("ocp-prod-us-east", cmds[0].CommandID, "EXECUTED", "Patched NooBaa CR on ocp-prod-us-east")
	if err != nil || acked.Status != "EXECUTED" {
		t.Fatalf("AckCommand failed: %v", err)
	}
}

func TestZeroConfigUnknownCRDIntrospectionAndFieldSelection(t *testing.T) {
	ctx := context.Background()
	k8sClient := k8s.NewClient()
	gitopsMgr := gitops.NewManager(k8sClient)
	remEngine := remediation.NewEngine(k8sClient, gitopsMgr)

	// Simulate an issue on a completely new, unknown Operator Custom Resource (e.g. RedisCluster.redis.spotahome.com/v1)
	// with NO YAML file in configs/adapters/ and NO Go struct.
	ev := types.DiagnosticEvent{
		Namespace:       "cache-prod",
		PodName:         "redis-cluster-0",
		OwnerAPIVersion: "redis.spotahome.com/v1",
		OwnerKind:       "RedisFailover",
		OwnerName:       "redis-cache",
		Reason:          "PVCFilesystemFull",
		Message:         "Redis persistence volume is 98% full",
		CurrentStorage:  "20Gi",
	}
	assessment, err := remEngine.EvaluateAndAct(ctx, ev)
	if err != nil {
		t.Fatalf("EvaluateAndAct failed for unknown CRD: %v", err)
	}
	if assessment.OperatorAdapter != "universal-crd-ai-adapter" {
		t.Fatalf("expected universal-crd-ai-adapter to handle unknown CRD, got %q", assessment.OperatorAdapter)
	}
	if len(assessment.PlannedActions) == 0 || assessment.PlannedActions[0].TargetKind != "RedisFailover" {
		t.Fatalf("expected action targeting RedisFailover CR, got %+v", assessment.PlannedActions)
	}
	if !strings.Contains(assessment.PlannedActions[0].PatchPayload, `"40Gi"`) {
		t.Fatalf("expected storage expanded to 40Gi in CR patch payload: %s", assessment.PlannedActions[0].PatchPayload)
	}

	// Verify Post-Remediation Effectiveness Monitor records VERIFIED_HEALTHY (100/100 score)
	w := watcher.NewWatcher(k8sClient, remEngine)
	alerts := watcher.AlertmanagerPayload{
		Status: "firing",
		Alerts: []watcher.AlertmanagerAlert{
			{
				Status:      "firing",
				Fingerprint: "fp-redis-1",
				Labels: map[string]string{
					"alertname": "KubePodOOMKilled",
					"namespace": "default",
					"pod":       "pg-cluster-1",
				},
			},
		},
	}
	incs, err := w.HandleAlertmanagerWebhook(ctx, alerts)
	if err != nil || len(incs) != 1 {
		t.Fatalf("HandleAlertmanagerWebhook failed: %v", err)
	}
	verified := w.VerifyRemediationEffectiveness(ctx)
	if len(verified) != 1 || verified[0].Effectiveness == nil || verified[0].Effectiveness.VerificationStatus != "VERIFIED_HEALTHY" || verified[0].Effectiveness.EffectivenessScore != 100 {
		t.Fatalf("expected EffectivenessReport VERIFIED_HEALTHY (score=100), got %+v", verified[0].Effectiveness)
	}
}

func TestRegistryDeclarativeAndAIFieldSelection(t *testing.T) {
	ctx := context.Background()

	// Create temporary directory with JSON and YAML adapter definitions
	tmpDir := t.TempDir()
	_ = os.Mkdir(filepath.Join(tmpDir, "subdir"), 0755)
	_ = os.WriteFile(filepath.Join(tmpDir, "ignore.txt"), []byte("ignore"), 0600)
	jsonSpec := `{
		"name": "strimzi-kafka",
		"description": "Strimzi Kafka Operator",
		"primaryGvk": {"apiVersion": "kafka.strimzi.io/v1beta2", "kind": "Kafka", "plural": "kafkas", "role": "rootController"},
		"podLabelKeys": ["strimzi.io/cluster"],
		"autoRemediateMemory": false,
		"crdSchemaFields": [
			{"path": "spec.kafka.resources.limits.memory", "type": "string"},
			{"path": "spec.kafka.resources.requests.memory", "type": "string"},
			{"path": "spec.kafka.storage.size", "type": "string"},
			{"path": "spec.walStorage.size", "type": "string"}
		]
	}`
	_ = os.WriteFile(filepath.Join(tmpDir, "strimzi.json"), []byte(jsonSpec), 0600)

	reg := adapters.NewRegistry(tmpDir)
	reg.Register(nil) // no-op
	if err := reg.RegisterDeclarativeSpec(adapters.DeclarativeAdapterSpec{}); err == nil {
		t.Fatalf("expected error for empty DeclarativeAdapterSpec")
	}
	// Replace existing adapter by name
	_ = reg.RegisterDeclarativeSpec(adapters.DeclarativeAdapterSpec{
		Name:                "strimzi-kafka",
		Description:         "Updated Strimzi Kafka",
		PrimaryGVK:          adapters.GVKDescriptor{APIVersion: "kafka.strimzi.io/v1beta2", Kind: "Kafka"},
		PodLabelKeys:        []string{"strimzi.io/cluster"},
		AutoRemediateMemory: false,
		CRDSchemaFields: []k8s.CRDSchemaField{
			{Path: "spec.kafka.resources.limits.memory", Type: "string"},
			{Path: "spec.kafka.resources.requests.memory", Type: "string"},
			{Path: "spec.kafka.storage.size", Type: "string"},
			{Path: "spec.walStorage.size", Type: "string"},
		},
	})
	meta := reg.ListMetadata()
	if len(meta) < 2 {
		t.Fatalf("expected >=2 adapters in ListMetadata, got %d", len(meta))
	}

	// Match via label
	matched := reg.Match(types.DiagnosticEvent{
		Namespace: "kafka",
		Labels:    map[string]string{"strimzi.io/cluster": "my-kafka"},
		Reason:    "OOMKilled",
		ExitCode:  137,
	})
	if matched == nil || matched.Name() != "strimzi-kafka" {
		t.Fatalf("expected strimzi-kafka match via label, got %v", matched)
	}

	// Match via ownershipChain
	matchedChain := reg.Match(types.DiagnosticEvent{
		Namespace: "kafka",
		OwnershipChain: []types.OwnerChainLink{
			{APIVersion: "kafka.strimzi.io/v1beta2", Kind: "Kafka", Name: "chain-kafka"},
		},
	})
	if matchedChain == nil || matchedChain.Name() != "strimzi-kafka" {
		t.Fatalf("expected strimzi-kafka match via OwnershipChain")
	}

	// Wildcard match via OwnershipChain for unknown CR
	matchedWildcard := reg.Match(types.DiagnosticEvent{
		Namespace: "custom",
		OwnershipChain: []types.OwnerChainLink{
			{APIVersion: "v1", Kind: "Pod", Name: "p-0"},
			{APIVersion: "example.io/v1", Kind: "CustomApp", Name: "my-app"},
		},
	})
	if matchedWildcard == nil || matchedWildcard.Name() != "universal-crd-ai-adapter" {
		t.Fatalf("expected universal-crd-ai-adapter match via OwnershipChain")
	}
	if reg.Match(types.DiagnosticEvent{OwnerAPIVersion: "apps/v1", OwnerKind: "Deployment"}) != nil {
		t.Fatalf("expected nil match for standard Deployment")
	}

	// BuildRemediationPlan with AutoRemediateMemory=false
	plan, err := matched.BuildRemediationPlan(ctx, nil, types.DiagnosticEvent{
		Namespace: "kafka",
		Labels:    map[string]string{"strimzi.io/cluster": "my-kafka"},
		Reason:    "OOMKilled",
		ExitCode:  137,
	})
	if err != nil || plan.PrimaryAction.Policy != types.PolicyApprovalRequired {
		t.Fatalf("expected APPROVAL_REQUIRED when AutoRemediateMemory=false, got %+v (%v)", plan, err)
	}

	// BuildRemediationPlan when exceeding Namespace ResourceQuota
	quotaPlan, err := matched.BuildRemediationPlan(ctx, nil, types.DiagnosticEvent{
		Namespace:        "kafka",
		OwnerName:        "my-kafka",
		Reason:           "OOMKilled",
		ExitCode:         137,
		CurrentMemoryLim: "1Gi",
		QuotaMemoryLimit: "2Gi",
		QuotaMemoryUsed:  "1536Mi",
	})
	if err != nil || quotaPlan.IssueType != types.IssueNamespaceQuotaExceed || len(quotaPlan.FollowUpActions) != 1 {
		t.Fatalf("expected IssueNamespaceQuotaExceed with follow-up action, got %+v (%v)", quotaPlan, err)
	}

	// BuildRemediationPlan for WAL storage issue
	walPlan, err := matched.BuildRemediationPlan(ctx, nil, types.DiagnosticEvent{
		Namespace: "kafka",
		OwnerName: "my-kafka",
		Reason:    "WALDiskFull",
		Message:   "WAL disk space exhausted",
	})
	if err != nil || walPlan.IssueType != types.IssuePVCCapacityExhausted {
		t.Fatalf("expected IssuePVCCapacityExhausted for WAL issue, got %+v (%v)", walPlan, err)
	}

	// Test askOllamaForCRDFields via mock Ollama HTTP server returning markdown-fenced JSON
	ollamaSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{
				{
					"message": map[string]string{
						"content": "```json\n{\"updatedFields\":{\"spec.kafka.resources.limits.memory\":\"2Gi\"},\"reasoning\":\"AI selected spec.kafka.resources.limits.memory\"}\n```",
					},
				},
			},
		})
	}))
	defer ollamaSrv.Close()
	t.Setenv("UPSTREAM_LLM_BASE_URL", ollamaSrv.URL)

	aiPlan, err := matched.BuildRemediationPlan(ctx, nil, types.DiagnosticEvent{
		Namespace: "kafka",
		OwnerName: "my-kafka",
		Reason:    "OOMKilled",
		ExitCode:  137,
	})
	if err != nil || aiPlan.UpdatedFields["spec.kafka.resources.limits.memory"] != "2Gi" || !strings.Contains(aiPlan.RootCauseAnalysis, "AI selected") {
		t.Fatalf("expected AI-selected fields from mock Ollama server, got %+v (%v)", aiPlan, err)
	}
}


