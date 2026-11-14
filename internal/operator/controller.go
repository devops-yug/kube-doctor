package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
)

// KubeDoctorCRSpec represents the spec of a kubedoctor.io/v1alpha1 KubeDoctor operator Custom Resource.
type KubeDoctorCRSpec struct {
	Mode              string `json:"mode,omitempty"`              // "standalone", "hub", "agent"
	ApprovalMode      string `json:"approvalMode,omitempty"`      // "IN_CLUSTER", "GITHUB_PR", "GITLAB_MR", "HYBRID"
	InClusterApproval *bool  `json:"inClusterApproval,omitempty"` // allow direct in-cluster approval in OpenShift Console
	ConsolePlugin     struct {
		Enabled bool `json:"enabled"`
	} `json:"consolePlugin,omitempty"`
	GitOps struct {
		GitHub *gitops.OAuthProviderConfig `json:"github,omitempty"`
		GitLab *gitops.OAuthProviderConfig `json:"gitlab,omitempty"`
	} `json:"gitops,omitempty"`
}

// ReconcileSummary reports the result of an Operator reconciliation cycle.
type ReconcileSummary struct {
	LastReconcileAt           time.Time `json:"lastReconcileAt"`
	ReconcileCount            int       `json:"reconcileCount"`
	KubeDoctorCRsReconciled   int       `json:"kubeDoctorCrsReconciled"`
	RemediationCRsReconciled  int       `json:"remediationCrsReconciled"`
	InClusterApprovalsApplied int       `json:"inClusterApprovalsApplied"`
	ConsolePluginEnabled      bool      `json:"consolePluginEnabled"`
	OperatorPhase             string    `json:"operatorPhase"`
}

// Controller implements the OpenShift 4.22+ / Kubernetes 1.35+ Operator Reconciler
// for kubedoctor.io/v1alpha1 KubeDoctor and RemediationRequest Custom Resources.
type Controller struct {
	k8sClient         *k8s.Client
	gitopsManager     *gitops.Manager
	remediationEngine *remediation.Engine
	interval          time.Duration

	mu      sync.RWMutex
	summary ReconcileSummary
}

// NewController creates a new OpenShift Operator Controller for kubedoctor.io/v1alpha1 CRDs.
func NewController(k8sClient *k8s.Client, gitopsMgr *gitops.Manager, remEngine *remediation.Engine) *Controller {
	return &Controller{
		k8sClient:         k8sClient,
		gitopsManager:     gitopsMgr,
		remediationEngine: remEngine,
		interval:          15 * time.Second,
		summary: ReconcileSummary{
			ConsolePluginEnabled: true,
			OperatorPhase:        "Ready",
		},
	}
}

// Start launches the background Operator reconciliation loop.
func (c *Controller) Start(ctx context.Context) {
	if os.Getenv("DISABLE_OPERATOR_CONTROLLER") == "true" {
		return
	}
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		_, _ = c.ReconcileOnce(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := c.ReconcileOnce(ctx); err != nil {
					log.Printf("[operator] reconcile error: %v", err)
				}
			}
		}
	}()
}

// Status returns the current Operator Controller reconciliation status.
func (c *Controller) Status() ReconcileSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.summary
}

// ReconcileOnce performs a single reconciliation pass over KubeDoctor and RemediationRequest CRs in the cluster.
func (c *Controller) ReconcileOnce(ctx context.Context) (ReconcileSummary, error) {
	now := time.Now().UTC()
	kdCount := 0
	remCount := 0
	approvedCount := 0

	if c.k8sClient != nil && !c.k8sClient.IsSimulated() {
		// 1. Reconcile KubeDoctor CRs (kubedoctor.io/v1alpha1, Kind=KubeDoctor)
		var kdList k8s.GenericK8sList
		if err := c.k8sClient.GetJSON(ctx, "/apis/kubedoctor.io/v1alpha1/kubedoctors", &kdList); err == nil {
			for _, item := range kdList.Items {
				kdCount++
				c.reconcileKubeDoctorCR(ctx, item)
			}
		}

		// 2. Reconcile RemediationRequest CRs (kubedoctor.io/v1alpha1, Kind=RemediationRequest)
		var remList k8s.GenericK8sList
		if err := c.k8sClient.GetJSON(ctx, "/apis/kubedoctor.io/v1alpha1/remediationrequests", &remList); err == nil {
			for _, item := range remList.Items {
				remCount++
				if c.reconcileRemediationRequestCR(ctx, item) {
					approvedCount++
				}
			}
		}
	}

	c.mu.Lock()
	c.summary.LastReconcileAt = now
	c.summary.ReconcileCount++
	c.summary.KubeDoctorCRsReconciled = kdCount
	c.summary.RemediationCRsReconciled = remCount
	c.summary.InClusterApprovalsApplied += approvedCount
	c.summary.OperatorPhase = "Succeeded"
	out := c.summary
	c.mu.Unlock()

	return out, nil
}

func (c *Controller) reconcileKubeDoctorCR(ctx context.Context, item k8s.GenericK8sObject) {
	rawSpec, _ := json.Marshal(item.Spec)
	var spec KubeDoctorCRSpec
	_ = json.Unmarshal(rawSpec, &spec)

	if c.gitopsManager != nil {
		inCluster := true
		if spec.InClusterApproval != nil {
			inCluster = *spec.InClusterApproval
		}
		if spec.ApprovalMode != "" || spec.InClusterApproval != nil {
			c.gitopsManager.SetApprovalMode(spec.ApprovalMode, inCluster)
		}
		if spec.GitOps.GitHub != nil {
			spec.GitOps.GitHub.Provider = "github"
			_, _ = c.gitopsManager.ConfigureProvider(*spec.GitOps.GitHub)
		}
		if spec.GitOps.GitLab != nil {
			spec.GitOps.GitLab.Provider = "gitlab"
			_, _ = c.gitopsManager.ConfigureProvider(*spec.GitOps.GitLab)
		}
	}

	c.mu.Lock()
	c.summary.ConsolePluginEnabled = spec.ConsolePlugin.Enabled
	c.mu.Unlock()

	ns := item.Metadata.Namespace
	if ns == "" {
		ns = "kube-doctor-system"
	}
	pendingCount := 0
	if c.gitopsManager != nil {
		for _, a := range c.gitopsManager.ListApprovals() {
			if a.Status == "PENDING" {
				pendingCount++
			}
		}
	}

	statusPatch, _ := json.Marshal(map[string]any{
		"status": map[string]any{
			"phase":                "Ready",
			"consolePluginEnabled": spec.ConsolePlugin.Enabled,
			"pendingApprovals":     pendingCount,
			"lastReconciledAt":     time.Now().UTC().Format(time.RFC3339),
			"conditions": []map[string]any{
				{
					"type":               "Available",
					"status":             "True",
					"reason":             "Reconciled",
					"message":            "kube-doctor operator and OpenShift ConsolePlugin are healthy",
					"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
				},
			},
		},
	})
	_ = c.k8sClient.PatchGenericResource(ctx, "kubedoctor.io/v1alpha1", "KubeDoctor", ns, item.Metadata.Name, string(statusPatch),
		fmt.Sprintf("Update KubeDoctor CR %s/%s status", ns, item.Metadata.Name))
}

func (c *Controller) reconcileRemediationRequestCR(ctx context.Context, item k8s.GenericK8sObject) bool {
	phase, _ := item.Status["phase"].(string)
	if strings.EqualFold(phase, "Executed") {
		return false
	}

	specApproved, _ := item.Spec["approved"].(bool)
	inClusterApproved := specApproved || strings.EqualFold(phase, "Approved")
	if !inClusterApproved {
		return false
	}

	approver, _ := item.Status["approvedBy"].(string)
	if approver == "" {
		if item.Metadata.Annotations != nil {
			approver = item.Metadata.Annotations["kubedoctor.io/approved-by"]
		}
	}
	if approver == "" {
		approver = "openshift-cluster-admin"
	}

	// First try matching an existing tracked ApprovalRequest in gitops.Manager
	var execFn func(ctx context.Context, act types.RemediationAction) error
	if c.remediationEngine != nil {
		execFn = c.remediationEngine.ExecuteApprovedAction
	}

	if c.gitopsManager != nil {
		if _, err := c.gitopsManager.ApproveAndExecute(ctx, item.Metadata.Name, approver, execFn); err == nil {
			return true
		}
	}

	// If the RemediationRequest CR was created directly in the cluster (outside gitops.Manager memory),
	// extract targetResource and patchPayload from .spec and execute it directly!
	targetMap, _ := item.Spec["targetResource"].(map[string]any)
	apiVer, _ := targetMap["apiVersion"].(string)
	kind, _ := targetMap["kind"].(string)
	ns, _ := targetMap["namespace"].(string)
	if ns == "" {
		ns = item.Metadata.Namespace
	}
	name, _ := targetMap["name"].(string)
	op, _ := item.Spec["operation"].(string)
	patchPayload, _ := item.Spec["patchPayload"].(string)

	if kind != "" && name != "" && execFn != nil {
		act := types.RemediationAction{
			TargetAPIVersion: apiVer,
			TargetKind:       kind,
			TargetNamespace:  ns,
			TargetName:       name,
			Operation:        op,
			PatchPayload:     patchPayload,
			Description:      fmt.Sprintf("Executed via OpenShift Operator in-cluster approval of RemediationRequest %s/%s", item.Metadata.Namespace, item.Metadata.Name),
		}
		if err := execFn(ctx, act); err == nil {
			now := time.Now().UTC().Format(time.RFC3339)
			statusPatch, _ := json.Marshal(map[string]any{
				"status": map[string]any{
					"phase":      "Executed",
					"approvedBy": approver,
					"executedAt": now,
				},
			})
			_ = c.k8sClient.PatchGenericResource(ctx, "kubedoctor.io/v1alpha1", "RemediationRequest", item.Metadata.Namespace, item.Metadata.Name, string(statusPatch),
				fmt.Sprintf("Mark RemediationRequest %s/%s as Executed", item.Metadata.Namespace, item.Metadata.Name))
			return true
		}
	}

	return false
}
