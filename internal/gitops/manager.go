package gitops

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

// Manager handles GitOps Pull Requests and kubedoctor.io/v1alpha1 RemediationRequest CRDs
// for cluster actions that require human/admin approval (e.g., Namespace ResourceQuota
// expansion and OpenShift 4.22+ MachineSet autoscaling).
type Manager struct {
	mu        sync.RWMutex
	approvals map[string]*types.ApprovalRequest
	prCounter int
	repoURL   string
	k8sClient *k8s.Client
}

// NewManager initializes the GitOps + CRD approval manager.
func NewManager(k8sClient *k8s.Client) *Manager {
	repoURL := os.Getenv("GITOPS_REPO_URL")
	if repoURL == "" {
		repoURL = "https://github.com/org/openshift-gitops-fleet"
	}
	return &Manager{
		approvals: make(map[string]*types.ApprovalRequest),
		repoURL:   repoURL,
		k8sClient: k8sClient,
	}
}

// CreateApprovalRequest creates both a RemediationRequest CRD in the cluster and a GitOps Pull Request
// for a privileged action (e.g. ResourceQuota update or OpenShift MachineSet scale), optionally chaining
// follow-up actions (e.g. StatefulSet memory update once the ResourceQuota is expanded).
func (m *Manager) CreateApprovalRequest(
	ctx context.Context,
	issueType types.IssueType,
	criticality types.Criticality,
	severity types.Severity,
	reason string,
	primaryAction types.RemediationAction,
	followUpActions []types.RemediationAction,
) (*types.ApprovalRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.prCounter++
	prNum := 100 + m.prCounter
	id := fmt.Sprintf("appr-%d-%d", time.Now().Unix(), m.prCounter)
	crdName := fmt.Sprintf("remediate-%s-%d", sanitizeName(primaryAction.TargetName), m.prCounter)
	branch := fmt.Sprintf("kube-doctor/%s/%s", string(issueType), crdName)
	prURL := fmt.Sprintf("%s/pull/%d", m.repoURL, prNum)

	ns := primaryAction.TargetNamespace
	if ns == "" {
		ns = "openshift-machine-api"
	}

	crdYAML := fmt.Sprintf(`apiVersion: kubedoctor.io/v1alpha1
kind: RemediationRequest
metadata:
  name: %s
  namespace: %s
  labels:
    kubedoctor.io/criticality: "%s"
    kubedoctor.io/severity: "%s"
    kubedoctor.io/issue-type: "%s"
spec:
  issueType: "%s"
  criticality: "%s"
  severity: "%s"
  reason: %q
  gitOpsPullRequest:
    url: "%s"
    branch: "%s"
    filePath: "%s"
  targetResource:
    apiVersion: "%s"
    kind: "%s"
    namespace: "%s"
    name: "%s"
  operation: "%s"
  patchPayload: |
    %s
status:
  phase: PendingApproval
`, crdName, ns, criticality, severity, issueType,
		issueType, criticality, severity, reason,
		prURL, branch, primaryAction.GitOpsFilePath,
		primaryAction.TargetAPIVersion, primaryAction.TargetKind, ns, primaryAction.TargetName,
		primaryAction.Operation, primaryAction.PatchPayload)

	crdJSONObj := map[string]any{
		"apiVersion": "kubedoctor.io/v1alpha1",
		"kind":       "RemediationRequest",
		"metadata": map[string]any{
			"name":      crdName,
			"namespace": ns,
		},
		"spec": map[string]any{
			"issueType":   issueType,
			"criticality": criticality,
			"severity":    severity,
			"reason":      reason,
			"gitOpsPR":    prURL,
			"action":      primaryAction,
			"followUps":   followUpActions,
		},
		"status": map[string]any{
			"phase": "PendingApproval",
		},
	}
	crdJSONBytes, _ := json.Marshal(crdJSONObj)
	_ = m.k8sClient.CreateRemediationRequestCRD(ctx, ns, crdName, string(crdJSONBytes))

	primaryAction.Status = "PENDING_APPROVAL"
	primaryAction.ApprovalID = id
	primaryAction.GitOpsPRURL = prURL

	req := &types.ApprovalRequest{
		ID:              id,
		CRDName:         crdName,
		Namespace:       ns,
		IssueType:       issueType,
		Criticality:     criticality,
		Severity:        severity,
		Reason:          reason,
		Action:          primaryAction,
		FollowUpActions: followUpActions,
		GitOpsPRNumber:  prNum,
		GitOpsPRURL:     prURL,
		GitOpsBranch:    branch,
		CRDManifestYAML: crdYAML,
		Status:          "PENDING",
		CreatedAt:       time.Now().UTC(),
		UpdatedAt:       time.Now().UTC(),
	}

	m.approvals[id] = req
	return req, nil
}

// ListApprovals returns all pending and historical approval requests.
func (m *Manager) ListApprovals() []*types.ApprovalRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*types.ApprovalRequest, 0, len(m.approvals))
	for _, v := range m.approvals {
		out = append(out, v)
	}
	return out
}

func (m *Manager) findApprovalLocked(idOrRef string) (*types.ApprovalRequest, bool) {
	if req, ok := m.approvals[idOrRef]; ok {
		return req, true
	}
	for _, req := range m.approvals {
		if req.CRDName == idOrRef || req.Action.ActionID == idOrRef ||
			(len(idOrRef) >= 8 && len(req.ID) > len(idOrRef) && req.ID[:len(idOrRef)] == idOrRef) {
			return req, true
		}
	}
	return nil, false
}

// GetApproval retrieves a specific approval request by ID, prefix, CRDName, or ActionID.
func (m *Manager) GetApproval(id string) (*types.ApprovalRequest, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.findApprovalLocked(id)
}

// ApproveAndExecute marks an approval request as APPROVED (e.g. via GitOps PR merge webhook,
// Copilot approval command, or admin API) and executes the cluster remediation + any chained follow-up actions.
func (m *Manager) ApproveAndExecute(ctx context.Context, id, approver string, executor func(ctx context.Context, act types.RemediationAction) error) (*types.ApprovalRequest, error) {
	m.mu.Lock()
	req, ok := m.findApprovalLocked(id)
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("approval request %q not found", id)
	}
	if req.Status == "APPROVED" {
		m.mu.Unlock()
		return req, nil
	}
	m.mu.Unlock()

	if err := executor(ctx, req.Action); err != nil {
		return nil, fmt.Errorf("failed executing primary approved action: %w", err)
	}

	now := time.Now().UTC()
	m.mu.Lock()
	req.Status = "APPROVED"
	req.ApprovedBy = approver
	req.UpdatedAt = now
	req.Action.Status = "EXECUTED"
	req.Action.ExecutedAt = &now

	for i := range req.FollowUpActions {
		if err := executor(ctx, req.FollowUpActions[i]); err == nil {
			req.FollowUpActions[i].Status = "EXECUTED"
			req.FollowUpActions[i].ExecutedAt = &now
		} else {
			req.FollowUpActions[i].Status = "FAILED"
		}
	}
	m.mu.Unlock()

	return req, nil
}

// Reject marks an approval request as REJECTED.
func (m *Manager) Reject(id, actor string) (*types.ApprovalRequest, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	req, ok := m.approvals[id]
	if !ok {
		return nil, fmt.Errorf("approval request %q not found", id)
	}
	req.Status = "REJECTED"
	req.ApprovedBy = actor
	req.UpdatedAt = time.Now().UTC()
	req.Action.Status = "REJECTED"
	return req, nil
}

func sanitizeName(s string) string {
	if s == "" {
		return "cluster-resource"
	}
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			out = append(out, r)
		} else if r >= 'A' && r <= 'Z' {
			out = append(out, r+('a'-'A'))
		} else {
			out = append(out, '-')
		}
	}
	return string(out)
}

