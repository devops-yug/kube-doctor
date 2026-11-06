package adapters_test

import (
	"context"
	"strings"
	"testing"

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
}

