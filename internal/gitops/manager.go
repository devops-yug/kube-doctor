package gitops

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

const (
	ApprovalModeInCluster = "IN_CLUSTER"
	ApprovalModeGitHub    = "GITHUB_PR"
	ApprovalModeGitLab    = "GITLAB_MR"
	ApprovalModeHybrid    = "HYBRID"
)

// Manager handles GitOps Pull Requests / Merge Requests (GitHub & GitLab OAuth Apps)
// and kubedoctor.io/v1alpha1 RemediationRequest CRDs for cluster actions that require
// human/admin approval (e.g., Namespace ResourceQuota expansion and OpenShift 4.22+ MachineSet autoscaling).
type Manager struct {
	mu               sync.RWMutex
	approvals        map[string]*types.ApprovalRequest
	prCounter        int
	repoURL          string
	approvalMode     string
	inClusterAllowed bool
	k8sClient        *k8s.Client
	httpClient       *http.Client
	githubConfig     *OAuthProviderConfig
	gitlabConfig     *OAuthProviderConfig
	oauthStates      map[string]string
}

// NewManager initializes the GitOps + CRD + OAuth SCM approval manager.
func NewManager(k8sClient *k8s.Client) *Manager {
	repoURL := strings.TrimRight(os.Getenv("GITOPS_REPO_URL"), "/")
	if repoURL == "" {
		repoURL = "https://github.com/org/openshift-gitops-fleet"
	}
	mode := strings.ToUpper(strings.TrimSpace(os.Getenv("KUBE_DOCTOR_APPROVAL_MODE")))
	if mode == "" {
		mode = ApprovalModeHybrid
	}
	inCluster := true
	if strings.EqualFold(os.Getenv("KUBE_DOCTOR_INCLUSTER_APPROVAL"), "false") {
		inCluster = false
	}

	ghToken := strings.TrimSpace(os.Getenv("GITHUB_OAUTH_TOKEN"))
	glToken := strings.TrimSpace(os.Getenv("GITLAB_OAUTH_TOKEN"))

	ghWebURL := defaultStr(os.Getenv("GITHUB_WEB_URL"), "https://github.com")
	ghAPIURL := defaultStr(os.Getenv("GITHUB_API_URL"), "https://api.github.com")
	glBaseURL := defaultStr(os.Getenv("GITLAB_BASE_URL"), "https://gitlab.com")

	mgr := &Manager{
		approvals:        make(map[string]*types.ApprovalRequest),
		repoURL:          repoURL,
		approvalMode:     mode,
		inClusterAllowed: inCluster,
		k8sClient:        k8sClient,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		githubConfig: &OAuthProviderConfig{
			Provider:     "github",
			ClientID:     os.Getenv("GITHUB_OAUTH_CLIENT_ID"),
			ClientSecret: os.Getenv("GITHUB_OAUTH_CLIENT_SECRET"),
			RedirectURI:  os.Getenv("GITHUB_OAUTH_REDIRECT_URI"),
			WebURL:       strings.TrimRight(ghWebURL, "/"),
			APIURL:       strings.TrimRight(ghAPIURL, "/"),
			Repository:   defaultStr(os.Getenv("GITHUB_GITOPS_REPO"), "org/openshift-gitops-fleet"),
			BaseBranch:   defaultStr(os.Getenv("GITHUB_BASE_BRANCH"), "main"),
			AccessToken:  ghToken,
			Connected:    ghToken != "",
		},
		gitlabConfig: &OAuthProviderConfig{
			Provider:     "gitlab",
			ClientID:     os.Getenv("GITLAB_OAUTH_CLIENT_ID"),
			ClientSecret: os.Getenv("GITLAB_OAUTH_CLIENT_SECRET"),
			RedirectURI:  os.Getenv("GITLAB_OAUTH_REDIRECT_URI"),
			WebURL:       strings.TrimRight(glBaseURL, "/"),
			APIURL:       strings.TrimRight(glBaseURL, "/") + "/api/v4",
			Repository:   defaultStr(os.Getenv("GITLAB_GITOPS_PROJECT"), "org/openshift-gitops-fleet"),
			BaseBranch:   defaultStr(os.Getenv("GITLAB_BASE_BRANCH"), "main"),
			AccessToken:  glToken,
			Connected:    glToken != "",
		},
		oauthStates: make(map[string]string),
	}
	if mgr.githubConfig.Connected {
		mgr.githubConfig.ConnectedUser = "github-oauth-app"
		mgr.githubConfig.ConnectedAt = time.Now().UTC()
	}
	if mgr.gitlabConfig.Connected {
		mgr.gitlabConfig.ConnectedUser = "gitlab-oauth-app"
		mgr.gitlabConfig.ConnectedAt = time.Now().UTC()
	}
	return mgr
}

// CreateApprovalRequest creates both a RemediationRequest CRD in the cluster and (when configured)
// a GitHub Pull Request or GitLab Merge Request via OAuth App credentials, optionally chaining
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
	m.prCounter++
	prNum := 100 + m.prCounter
	id := fmt.Sprintf("appr-%d-%d", time.Now().Unix(), m.prCounter)
	crdName := fmt.Sprintf("remediate-%s-%d", sanitizeName(primaryAction.TargetName), m.prCounter)
	branch := fmt.Sprintf("kube-doctor/%s/%s", string(issueType), crdName)
	prURL := fmt.Sprintf("%s/pull/%d", m.repoURL, prNum)
	approvalMode := m.approvalMode
	inClusterAllowed := m.inClusterAllowed
	provider := "in-cluster"
	if m.githubConfig != nil && m.githubConfig.Connected && (approvalMode == ApprovalModeGitHub || approvalMode == ApprovalModeHybrid) {
		provider = "github"
	} else if m.gitlabConfig != nil && m.gitlabConfig.Connected && (approvalMode == ApprovalModeGitLab || approvalMode == ApprovalModeHybrid) {
		provider = "gitlab"
		prURL = fmt.Sprintf("%s/%s/-/merge_requests/%d", m.gitlabConfig.WebURL, m.gitlabConfig.Repository, prNum)
	}
	m.mu.Unlock()

	ns := primaryAction.TargetNamespace
	if ns == "" {
		ns = "openshift-machine-api"
	}
	filePath := primaryAction.GitOpsFilePath
	if filePath == "" {
		filePath = fmt.Sprintf("clusters/ocp-4.22/%s/%s-%s.yaml", ns, strings.ToLower(primaryAction.TargetKind), sanitizeName(primaryAction.TargetName))
		primaryAction.GitOpsFilePath = filePath
	}

	// Attempt live GitHub PR or GitLab MR creation if OAuth provider is connected
	if provider == "github" {
		if createdURL, createdNum, err := m.createGitHubPullRequest(ctx, branch, filePath, issueType, criticality, severity, reason, primaryAction, followUpActions); err == nil && createdURL != "" {
			prURL = createdURL
			if createdNum > 0 {
				prNum = createdNum
			}
		}
	} else if provider == "gitlab" {
		if createdURL, createdNum, err := m.createGitLabMergeRequest(ctx, branch, filePath, issueType, criticality, severity, reason, primaryAction, followUpActions); err == nil && createdURL != "" {
			prURL = createdURL
			if createdNum > 0 {
				prNum = createdNum
			}
		}
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
    kubedoctor.io/approval-mode: "%s"
spec:
  issueType: "%s"
  criticality: "%s"
  severity: "%s"
  reason: %q
  approvalMode: "%s"
  inClusterAllowed: %v
  approved: false
  gitOpsPullRequest:
    provider: "%s"
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
`, crdName, ns, criticality, severity, issueType, approvalMode,
		issueType, criticality, severity, reason, approvalMode, inClusterAllowed,
		provider, prURL, branch, filePath,
		primaryAction.TargetAPIVersion, primaryAction.TargetKind, ns, primaryAction.TargetName,
		primaryAction.Operation, primaryAction.PatchPayload)

	crdJSONObj := map[string]any{
		"apiVersion": "kubedoctor.io/v1alpha1",
		"kind":       "RemediationRequest",
		"metadata": map[string]any{
			"name":      crdName,
			"namespace": ns,
			"labels": map[string]string{
				"kubedoctor.io/approval-id":   id,
				"kubedoctor.io/criticality":   string(criticality),
				"kubedoctor.io/severity":      string(severity),
				"kubedoctor.io/approval-mode": approvalMode,
			},
		},
		"spec": map[string]any{
			"issueType":        issueType,
			"criticality":      criticality,
			"severity":         severity,
			"reason":           reason,
			"approvalMode":     approvalMode,
			"inClusterAllowed": inClusterAllowed,
			"approved":         false,
			"gitOpsPR":         prURL,
			"gitOpsPullRequest": map[string]any{
				"provider": provider,
				"url":      prURL,
				"branch":   branch,
				"filePath": filePath,
			},
			"targetResource": map[string]any{
				"apiVersion": primaryAction.TargetAPIVersion,
				"kind":       primaryAction.TargetKind,
				"namespace":  ns,
				"name":       primaryAction.TargetName,
			},
			"operation":    primaryAction.Operation,
			"patchPayload": primaryAction.PatchPayload,
			"action":       primaryAction,
			"followUps":    followUpActions,
		},
		"status": map[string]any{
			"phase": "PendingApproval",
		},
	}
	crdJSONBytes, _ := json.Marshal(crdJSONObj)
	if m.k8sClient != nil {
		_ = m.k8sClient.CreateRemediationRequestCRD(ctx, ns, crdName, string(crdJSONBytes))
	}

	primaryAction.Status = "PENDING_APPROVAL"
	primaryAction.ApprovalID = id
	primaryAction.GitOpsPRURL = prURL

	req := &types.ApprovalRequest{
		ID:               id,
		CRDName:          crdName,
		Namespace:        ns,
		IssueType:        issueType,
		Criticality:      criticality,
		Severity:         severity,
		Reason:           reason,
		ApprovalMode:     approvalMode,
		GitProvider:      provider,
		InClusterAllowed: inClusterAllowed,
		Action:           primaryAction,
		FollowUpActions:  followUpActions,
		GitOpsPRNumber:   prNum,
		GitOpsPRURL:      prURL,
		GitOpsBranch:     branch,
		CRDManifestYAML:  crdYAML,
		Status:           "PENDING",
		CreatedAt:        time.Now().UTC(),
		UpdatedAt:        time.Now().UTC(),
	}

	m.mu.Lock()
	m.approvals[id] = req
	m.mu.Unlock()
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

// ApproveAndExecute marks an approval request as APPROVED (e.g. via OpenShift Console in-cluster approval,
// RemediationRequest CRD reconciliation, GitOps PR/MR merge webhook, or Copilot command) and executes
// the cluster remediation + any chained follow-up actions.
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

	if executor != nil {
		if err := executor(ctx, req.Action); err != nil {
			return nil, fmt.Errorf("failed executing primary approved action: %w", err)
		}
	}

	now := time.Now().UTC()
	m.mu.Lock()
	req.Status = "APPROVED"
	req.ApprovedBy = approver
	req.UpdatedAt = now
	req.Action.Status = "EXECUTED"
	req.Action.ExecutedAt = &now

	for i := range req.FollowUpActions {
		if executor == nil {
			req.FollowUpActions[i].Status = "EXECUTED"
			req.FollowUpActions[i].ExecutedAt = &now
			continue
		}
		if err := executor(ctx, req.FollowUpActions[i]); err == nil {
			req.FollowUpActions[i].Status = "EXECUTED"
			req.FollowUpActions[i].ExecutedAt = &now
		} else {
			req.FollowUpActions[i].Status = "FAILED"
		}
	}
	crdName := req.CRDName
	ns := req.Namespace
	m.mu.Unlock()

	if m.k8sClient != nil && crdName != "" {
		statusPatch, _ := json.Marshal(map[string]any{
			"spec": map[string]any{
				"approved": true,
			},
			"status": map[string]any{
				"phase":      "Executed",
				"approvedBy": approver,
				"executedAt": now.Format(time.RFC3339),
			},
		})
		_ = m.k8sClient.PatchGenericResource(ctx, "kubedoctor.io/v1alpha1", "RemediationRequest", ns, crdName, string(statusPatch),
			fmt.Sprintf("Mark RemediationRequest %s/%s as Executed (approvedBy=%s)", ns, crdName, approver))
	}

	return req, nil
}

// Reject marks an approval request as REJECTED and updates the in-cluster RemediationRequest CRD status.
func (m *Manager) Reject(id, actor string) (*types.ApprovalRequest, error) {
	m.mu.Lock()
	req, ok := m.findApprovalLocked(id)
	if !ok {
		m.mu.Unlock()
		return nil, fmt.Errorf("approval request %q not found", id)
	}
	now := time.Now().UTC()
	req.Status = "REJECTED"
	req.ApprovedBy = actor
	req.UpdatedAt = now
	req.Action.Status = "REJECTED"
	crdName := req.CRDName
	ns := req.Namespace
	m.mu.Unlock()

	if m.k8sClient != nil && crdName != "" {
		statusPatch, _ := json.Marshal(map[string]any{
			"status": map[string]any{
				"phase":      "Rejected",
				"approvedBy": actor,
			},
		})
		_ = m.k8sClient.PatchGenericResource(context.Background(), "kubedoctor.io/v1alpha1", "RemediationRequest", ns, crdName, string(statusPatch),
			fmt.Sprintf("Mark RemediationRequest %s/%s as Rejected (actor=%s)", ns, crdName, actor))
	}
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

func defaultStr(val, fallback string) string {
	if strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return fallback
}
