package gitops

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/luigi/kube-doctor/internal/types"
)

// OAuthProviderConfig holds OAuth 2.0 Application credentials and repository settings
// for GitHub OAuth Apps or GitLab OAuth Applications.
type OAuthProviderConfig struct {
	Provider      string    `json:"provider"`               // "github" or "gitlab"
	ClientID      string    `json:"clientId"`               // OAuth App Client ID / Application ID
	ClientSecret  string    `json:"clientSecret,omitempty"` // OAuth App Client Secret
	RedirectURI   string    `json:"redirectUri,omitempty"`  // OAuth Callback URL
	WebURL        string    `json:"webUrl,omitempty"`       // e.g. "https://github.com" or "https://gitlab.com"
	APIURL        string    `json:"apiUrl,omitempty"`       // e.g. "https://api.github.com" or "https://gitlab.com/api/v4"
	Repository    string    `json:"repository"`             // e.g. "org/openshift-gitops-fleet"
	BaseBranch    string    `json:"baseBranch"`             // e.g. "main"
	AccessToken   string    `json:"accessToken,omitempty"`  // OAuth access token or PAT
	RefreshToken  string    `json:"refreshToken,omitempty"`
	Connected     bool      `json:"connected"`
	ConnectedUser string    `json:"connectedUser,omitempty"`
	ConnectedAt   time.Time `json:"connectedAt,omitempty"`
}

// OAuthProviderStatus is the sanitized status representation returned to the OpenShift UI plugin.
type OAuthProviderStatus struct {
	Provider      string    `json:"provider"`
	ClientID      string    `json:"clientId"`
	HasSecret     bool      `json:"hasSecret"`
	RedirectURI   string    `json:"redirectUri,omitempty"`
	WebURL        string    `json:"webUrl"`
	APIURL        string    `json:"apiUrl"`
	Repository    string    `json:"repository"`
	BaseBranch    string    `json:"baseBranch"`
	Connected     bool      `json:"connected"`
	ConnectedUser string    `json:"connectedUser,omitempty"`
	ConnectedAt   time.Time `json:"connectedAt,omitempty"`
}

// IntegrationStatus summarizes the cluster approval governance mode and GitHub/GitLab OAuth connections.
type IntegrationStatus struct {
	ApprovalMode     string              `json:"approvalMode"`     // "IN_CLUSTER", "GITHUB_PR", "GITLAB_MR", "HYBRID"
	InClusterAllowed bool                `json:"inClusterAllowed"` // true when resources managed in-cluster can be approved directly in OpenShift Console
	GitHub           OAuthProviderStatus `json:"github"`
	GitLab           OAuthProviderStatus `json:"gitlab"`
}

// SetHTTPClient overrides the HTTP client used for GitHub/GitLab OAuth and REST API calls (useful in tests).
func (m *Manager) SetHTTPClient(hc *http.Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if hc != nil {
		m.httpClient = hc
	}
}

// SetApprovalMode updates the governance approval mode and whether direct in-cluster approval is allowed.
func (m *Manager) SetApprovalMode(mode string, inClusterAllowed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	norm := strings.ToUpper(strings.TrimSpace(mode))
	switch norm {
	case ApprovalModeInCluster, ApprovalModeGitHub, ApprovalModeGitLab, ApprovalModeHybrid:
		m.approvalMode = norm
	default:
		m.approvalMode = ApprovalModeHybrid
	}
	m.inClusterAllowed = inClusterAllowed
}

// GetIntegrationStatus returns the current approval governance settings and GitHub/GitLab OAuth status.
func (m *Manager) GetIntegrationStatus() IntegrationStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return IntegrationStatus{
		ApprovalMode:     m.approvalMode,
		InClusterAllowed: m.inClusterAllowed,
		GitHub:           toProviderStatus(m.githubConfig),
		GitLab:           toProviderStatus(m.gitlabConfig),
	}
}

func toProviderStatus(cfg *OAuthProviderConfig) OAuthProviderStatus {
	if cfg == nil {
		return OAuthProviderStatus{}
	}
	return OAuthProviderStatus{
		Provider:      cfg.Provider,
		ClientID:      cfg.ClientID,
		HasSecret:     cfg.ClientSecret != "" || cfg.AccessToken != "",
		RedirectURI:   cfg.RedirectURI,
		WebURL:        cfg.WebURL,
		APIURL:        cfg.APIURL,
		Repository:    cfg.Repository,
		BaseBranch:    cfg.BaseBranch,
		Connected:     cfg.Connected,
		ConnectedUser: cfg.ConnectedUser,
		ConnectedAt:   cfg.ConnectedAt,
	}
}

// ConfigureProvider updates the GitHub or GitLab OAuth App configuration (and optional direct token).
func (m *Manager) ConfigureProvider(cfg OAuthProviderConfig) (*OAuthProviderStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	provider := strings.ToLower(strings.TrimSpace(cfg.Provider))
	var target *OAuthProviderConfig
	switch provider {
	case "github":
		target = m.githubConfig
		if cfg.WebURL == "" && target.WebURL == "" {
			target.WebURL = "https://github.com"
		}
		if cfg.APIURL == "" && target.APIURL == "" {
			target.APIURL = "https://api.github.com"
		}
	case "gitlab":
		target = m.gitlabConfig
		if cfg.WebURL == "" && target.WebURL == "" {
			target.WebURL = "https://gitlab.com"
		}
		if cfg.APIURL == "" && target.APIURL == "" {
			target.APIURL = strings.TrimRight(target.WebURL, "/") + "/api/v4"
		}
	default:
		return nil, fmt.Errorf("unsupported gitops provider %q (expected 'github' or 'gitlab')", cfg.Provider)
	}

	if cfg.ClientID != "" {
		target.ClientID = strings.TrimSpace(cfg.ClientID)
	}
	if cfg.ClientSecret != "" {
		target.ClientSecret = strings.TrimSpace(cfg.ClientSecret)
	}
	if cfg.RedirectURI != "" {
		target.RedirectURI = strings.TrimSpace(cfg.RedirectURI)
	}
	if cfg.WebURL != "" {
		target.WebURL = strings.TrimRight(strings.TrimSpace(cfg.WebURL), "/")
		if provider == "gitlab" && cfg.APIURL == "" {
			target.APIURL = target.WebURL + "/api/v4"
		}
	}
	if cfg.APIURL != "" {
		target.APIURL = strings.TrimRight(strings.TrimSpace(cfg.APIURL), "/")
	}
	if cfg.Repository != "" {
		target.Repository = strings.Trim(strings.TrimSpace(cfg.Repository), "/")
	}
	if cfg.BaseBranch != "" {
		target.BaseBranch = strings.TrimSpace(cfg.BaseBranch)
	}
	if cfg.AccessToken != "" {
		target.AccessToken = strings.TrimSpace(cfg.AccessToken)
		target.Connected = true
		if cfg.ConnectedUser != "" {
			target.ConnectedUser = cfg.ConnectedUser
		} else if target.ConnectedUser == "" {
			target.ConnectedUser = provider + "-oauth-user"
		}
		target.ConnectedAt = time.Now().UTC()
	}

	st := toProviderStatus(target)
	return &st, nil
}

// GetOAuthAuthorizeURL generates the OAuth 2.0 authorization URL and CSRF state token for GitHub or GitLab.
func (m *Manager) GetOAuthAuthorizeURL(provider, redirectURI string) (authorizeURL, state string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	p := strings.ToLower(strings.TrimSpace(provider))
	stateBytes := make([]byte, 16)
	_, _ = rand.Read(stateBytes)
	state = hex.EncodeToString(stateBytes)
	m.oauthStates[state] = p

	switch p {
	case "github":
		cfg := m.githubConfig
		clientID := defaultStr(cfg.ClientID, "kube-doctor-github-oauth-client")
		redir := defaultStr(redirectURI, cfg.RedirectURI)
		q := url.Values{}
		q.Set("client_id", clientID)
		q.Set("scope", "repo read:user")
		q.Set("state", state)
		if redir != "" {
			q.Set("redirect_uri", redir)
		}
		return fmt.Sprintf("%s/login/oauth/authorize?%s", cfg.WebURL, q.Encode()), state, nil

	case "gitlab":
		cfg := m.gitlabConfig
		clientID := defaultStr(cfg.ClientID, "kube-doctor-gitlab-oauth-client")
		redir := defaultStr(redirectURI, cfg.RedirectURI)
		q := url.Values{}
		q.Set("client_id", clientID)
		q.Set("response_type", "code")
		q.Set("scope", "api read_user")
		q.Set("state", state)
		if redir != "" {
			q.Set("redirect_uri", redir)
		}
		return fmt.Sprintf("%s/oauth/authorize?%s", cfg.WebURL, q.Encode()), state, nil

	default:
		return "", "", fmt.Errorf("unsupported OAuth provider %q (expected 'github' or 'gitlab')", provider)
	}
}

// ExchangeOAuthCode completes the OAuth 2.0 Web Application flow for GitHub or GitLab by exchanging
// the authorization code for an access token and verifying the authenticated user identity.
func (m *Manager) ExchangeOAuthCode(ctx context.Context, provider, code, state string) (*OAuthProviderStatus, error) {
	p := strings.ToLower(strings.TrimSpace(provider))
	if strings.TrimSpace(code) == "" {
		return nil, fmt.Errorf("authorization code is required")
	}

	m.mu.Lock()
	if state != "" && len(m.oauthStates) > 0 {
		if expectedProvider, exists := m.oauthStates[state]; exists {
			delete(m.oauthStates, state)
			if expectedProvider != p {
				m.mu.Unlock()
				return nil, fmt.Errorf("oauth state provider mismatch: expected %q, got %q", expectedProvider, p)
			}
		}
	}
	var cfgCopy OAuthProviderConfig
	switch p {
	case "github":
		cfgCopy = *m.githubConfig
	case "gitlab":
		cfgCopy = *m.gitlabConfig
	default:
		m.mu.Unlock()
		return nil, fmt.Errorf("unsupported OAuth provider %q", provider)
	}
	hc := m.httpClient
	m.mu.Unlock()

	// Support deterministic simulated OAuth exchange for local/demo OpenShift Console flows when code has "sim-" prefix
	if strings.HasPrefix(code, "sim-") {
		m.mu.Lock()
		defer m.mu.Unlock()
		target := m.githubConfig
		if p == "gitlab" {
			target = m.gitlabConfig
		}
		target.AccessToken = "oauth2_" + p + "_" + code
		target.Connected = true
		target.ConnectedUser = p + "-sre-admin"
		target.ConnectedAt = time.Now().UTC()
		st := toProviderStatus(target)
		return &st, nil
	}

	var token, refreshToken, username string
	switch p {
	case "github":
		tokPayload := map[string]string{
			"client_id":     cfgCopy.ClientID,
			"client_secret": cfgCopy.ClientSecret,
			"code":          code,
			"redirect_uri":  cfgCopy.RedirectURI,
		}
		bodyBytes, _ := json.Marshal(tokPayload)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfgCopy.WebURL+"/login/oauth/access_token", bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		resp, err := hc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("github oauth token exchange failed: %w", err)
		}
		defer resp.Body.Close()
		var tokResp struct {
			AccessToken string `json:"access_token"`
			Error       string `json:"error"`
			ErrorDesc   string `json:"error_description"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&tokResp); err != nil {
			return nil, fmt.Errorf("invalid github oauth response: %w", err)
		}
		if tokResp.Error != "" || tokResp.AccessToken == "" {
			return nil, fmt.Errorf("github oauth error: %s (%s)", tokResp.Error, tokResp.ErrorDesc)
		}
		token = tokResp.AccessToken
		username = m.fetchGitHubUser(ctx, hc, cfgCopy.APIURL, token)

	case "gitlab":
		form := url.Values{}
		form.Set("client_id", cfgCopy.ClientID)
		form.Set("client_secret", cfgCopy.ClientSecret)
		form.Set("code", code)
		form.Set("grant_type", "authorization_code")
		form.Set("redirect_uri", cfgCopy.RedirectURI)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfgCopy.WebURL+"/oauth/token", strings.NewReader(form.Encode()))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set("Accept", "application/json")
		resp, err := hc.Do(req)
		if err != nil {
			return nil, fmt.Errorf("gitlab oauth token exchange failed: %w", err)
		}
		defer resp.Body.Close()
		var tokResp struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			Error        string `json:"error"`
			ErrorDesc    string `json:"error_description"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&tokResp); err != nil {
			return nil, fmt.Errorf("invalid gitlab oauth response: %w", err)
		}
		if tokResp.Error != "" || tokResp.AccessToken == "" {
			return nil, fmt.Errorf("gitlab oauth error: %s (%s)", tokResp.Error, tokResp.ErrorDesc)
		}
		token = tokResp.AccessToken
		refreshToken = tokResp.RefreshToken
		username = m.fetchGitLabUser(ctx, hc, cfgCopy.APIURL, token)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	target := m.githubConfig
	if p == "gitlab" {
		target = m.gitlabConfig
	}
	target.AccessToken = token
	target.RefreshToken = refreshToken
	target.Connected = true
	target.ConnectedUser = defaultStr(username, p+"-authenticated-user")
	target.ConnectedAt = time.Now().UTC()
	st := toProviderStatus(target)
	return &st, nil
}

// DisconnectProvider revokes/clears the stored OAuth access token for GitHub or GitLab.
func (m *Manager) DisconnectProvider(provider string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "github":
		m.githubConfig.AccessToken = ""
		m.githubConfig.RefreshToken = ""
		m.githubConfig.Connected = false
		m.githubConfig.ConnectedUser = ""
		return nil
	case "gitlab":
		m.gitlabConfig.AccessToken = ""
		m.gitlabConfig.RefreshToken = ""
		m.gitlabConfig.Connected = false
		m.gitlabConfig.ConnectedUser = ""
		return nil
	default:
		return fmt.Errorf("unsupported provider %q", provider)
	}
}

// CreateSCMRequestForApproval creates (or updates) a GitHub Pull Request or GitLab Merge Request
// for an existing ApprovalRequest (e.g., triggered from the OpenShift Console UI plugin).
func (m *Manager) CreateSCMRequestForApproval(ctx context.Context, approvalID, provider string) (*types.ApprovalRequest, error) {
	m.mu.RLock()
	req, ok := m.findApprovalLocked(approvalID)
	if !ok {
		m.mu.RUnlock()
		return nil, fmt.Errorf("approval request %q not found", approvalID)
	}
	reqCopy := *req
	ghCfg := *m.githubConfig
	glCfg := *m.gitlabConfig
	m.mu.RUnlock()

	p := strings.ToLower(strings.TrimSpace(provider))
	if p == "" {
		if ghCfg.Connected {
			p = "github"
		} else if glCfg.Connected {
			p = "gitlab"
		} else {
			p = "github"
		}
	}

	filePath := defaultStr(reqCopy.Action.GitOpsFilePath, fmt.Sprintf("clusters/ocp-4.22/%s/%s-%s.yaml", reqCopy.Namespace, strings.ToLower(reqCopy.Action.TargetKind), sanitizeName(reqCopy.Action.TargetName)))

	var prURL string
	var prNum int
	var err error
	switch p {
	case "github":
		prURL, prNum, err = m.createGitHubPullRequest(ctx, reqCopy.GitOpsBranch, filePath, reqCopy.IssueType, reqCopy.Criticality, reqCopy.Severity, reqCopy.Reason, reqCopy.Action, reqCopy.FollowUpActions)
		if err != nil || prURL == "" {
			prNum = reqCopy.GitOpsPRNumber
			prURL = fmt.Sprintf("%s/%s/pull/%d", ghCfg.WebURL, ghCfg.Repository, prNum)
		}
	case "gitlab":
		prURL, prNum, err = m.createGitLabMergeRequest(ctx, reqCopy.GitOpsBranch, filePath, reqCopy.IssueType, reqCopy.Criticality, reqCopy.Severity, reqCopy.Reason, reqCopy.Action, reqCopy.FollowUpActions)
		if err != nil || prURL == "" {
			prNum = reqCopy.GitOpsPRNumber
			prURL = fmt.Sprintf("%s/%s/-/merge_requests/%d", glCfg.WebURL, glCfg.Repository, prNum)
		}
	default:
		return nil, fmt.Errorf("unsupported SCM provider %q (expected 'github' or 'gitlab')", provider)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if liveReq, ok := m.findApprovalLocked(approvalID); ok {
		liveReq.GitProvider = p
		liveReq.GitOpsPRURL = prURL
		if prNum > 0 {
			liveReq.GitOpsPRNumber = prNum
		}
		liveReq.Action.GitOpsPRURL = prURL
		liveReq.UpdatedAt = time.Now().UTC()
		return liveReq, nil
	}
	return &reqCopy, nil
}

func (m *Manager) createGitHubPullRequest(
	ctx context.Context,
	branch, filePath string,
	issueType types.IssueType,
	criticality types.Criticality,
	severity types.Severity,
	reason string,
	primaryAction types.RemediationAction,
	followUpActions []types.RemediationAction,
) (string, int, error) {
	m.mu.RLock()
	cfg := *m.githubConfig
	hc := m.httpClient
	m.mu.RUnlock()

	if !cfg.Connected || cfg.AccessToken == "" || strings.HasPrefix(cfg.AccessToken, "oauth2_github_sim-") {
		return fmt.Sprintf("%s/%s/pull/%d", cfg.WebURL, cfg.Repository, 100+m.prCounter), 100 + m.prCounter, nil
	}

	baseBranch := defaultStr(cfg.BaseBranch, "main")
	repo := cfg.Repository

	// 1. Get base branch SHA
	refURL := fmt.Sprintf("%s/repos/%s/git/ref/heads/%s", cfg.APIURL, repo, baseBranch)
	refReq, err := http.NewRequestWithContext(ctx, http.MethodGet, refURL, nil)
	if err != nil {
		return "", 0, err
	}
	refReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	refReq.Header.Set("Accept", "application/vnd.github+json")
	refResp, err := hc.Do(refReq)
	if err != nil {
		return "", 0, err
	}
	defer refResp.Body.Close()
	var refData struct {
		Object struct {
			SHA string `json:"sha"`
		} `json:"object"`
	}
	_ = json.NewDecoder(refResp.Body).Decode(&refData)

	// 2. Create branch ref
	if refData.Object.SHA != "" {
		createRefBody, _ := json.Marshal(map[string]string{
			"ref": "refs/heads/" + branch,
			"sha": refData.Object.SHA,
		})
		cReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/repos/%s/git/refs", cfg.APIURL, repo), bytes.NewReader(createRefBody))
		cReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
		cReq.Header.Set("Content-Type", "application/json")
		if cResp, cErr := hc.Do(cReq); cErr == nil {
			_, _ = io.ReadAll(cResp.Body)
			_ = cResp.Body.Close()
		}
	}

	// 3. Commit remediation manifest file
	contentB64 := base64.StdEncoding.EncodeToString([]byte(primaryAction.PatchPayload))
	putBody, _ := json.Marshal(map[string]string{
		"message": fmt.Sprintf("[kube-doctor] %s on %s/%s (%s)", primaryAction.Operation, primaryAction.TargetKind, primaryAction.TargetName, issueType),
		"content": contentB64,
		"branch":  branch,
	})
	putReq, _ := http.NewRequestWithContext(ctx, http.MethodPut, fmt.Sprintf("%s/repos/%s/contents/%s", cfg.APIURL, repo, strings.TrimPrefix(filePath, "/")), bytes.NewReader(putBody))
	putReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	putReq.Header.Set("Content-Type", "application/json")
	if putResp, pErr := hc.Do(putReq); pErr == nil {
		_, _ = io.ReadAll(putResp.Body)
		_ = putResp.Body.Close()
	}

	// 4. Open Pull Request
	prTitle := fmt.Sprintf("[kube-doctor][%s/%s] %s on %s/%s", criticality, severity, primaryAction.Operation, primaryAction.TargetKind, primaryAction.TargetName)
	prBodyText := buildPRDescription(issueType, criticality, severity, reason, primaryAction, followUpActions)
	prPayload, _ := json.Marshal(map[string]string{
		"title": prTitle,
		"head":  branch,
		"base":  baseBranch,
		"body":  prBodyText,
	})
	prReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/repos/%s/pulls", cfg.APIURL, repo), bytes.NewReader(prPayload))
	if err != nil {
		return "", 0, err
	}
	prReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	prReq.Header.Set("Content-Type", "application/json")
	prResp, err := hc.Do(prReq)
	if err != nil {
		return "", 0, err
	}
	defer prResp.Body.Close()
	if prResp.StatusCode >= 300 {
		raw, _ := io.ReadAll(prResp.Body)
		return "", 0, fmt.Errorf("github PR creation returned %d: %s", prResp.StatusCode, string(raw))
	}
	var prOut struct {
		Number  int    `json:"number"`
		HTMLURL string `json:"html_url"`
	}
	if err := json.NewDecoder(prResp.Body).Decode(&prOut); err != nil {
		return "", 0, err
	}
	return prOut.HTMLURL, prOut.Number, nil
}

func (m *Manager) createGitLabMergeRequest(
	ctx context.Context,
	branch, filePath string,
	issueType types.IssueType,
	criticality types.Criticality,
	severity types.Severity,
	reason string,
	primaryAction types.RemediationAction,
	followUpActions []types.RemediationAction,
) (string, int, error) {
	m.mu.RLock()
	cfg := *m.gitlabConfig
	hc := m.httpClient
	m.mu.RUnlock()

	if !cfg.Connected || cfg.AccessToken == "" || strings.HasPrefix(cfg.AccessToken, "oauth2_gitlab_sim-") {
		return fmt.Sprintf("%s/%s/-/merge_requests/%d", cfg.WebURL, cfg.Repository, 100+m.prCounter), 100 + m.prCounter, nil
	}

	baseBranch := defaultStr(cfg.BaseBranch, "main")
	encodedProject := url.PathEscape(cfg.Repository)

	// 1. Create branch in GitLab
	branchURL := fmt.Sprintf("%s/projects/%s/repository/branches?branch=%s&ref=%s",
		cfg.APIURL, encodedProject, url.QueryEscape(branch), url.QueryEscape(baseBranch))
	bReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, branchURL, nil)
	bReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	if bResp, bErr := hc.Do(bReq); bErr == nil {
		_, _ = io.ReadAll(bResp.Body)
		_ = bResp.Body.Close()
	}

	// 2. Commit file in GitLab
	commitPayload, _ := json.Marshal(map[string]any{
		"branch":         branch,
		"commit_message": fmt.Sprintf("[kube-doctor] %s on %s/%s (%s)", primaryAction.Operation, primaryAction.TargetKind, primaryAction.TargetName, issueType),
		"actions": []map[string]string{
			{
				"action":    "create",
				"file_path": strings.TrimPrefix(filePath, "/"),
				"content":   primaryAction.PatchPayload,
			},
		},
	})
	cReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/projects/%s/repository/commits", cfg.APIURL, encodedProject), bytes.NewReader(commitPayload))
	cReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	cReq.Header.Set("Content-Type", "application/json")
	if cResp, cErr := hc.Do(cReq); cErr == nil {
		_, _ = io.ReadAll(cResp.Body)
		_ = cResp.Body.Close()
	}

	// 3. Create Merge Request in GitLab
	mrTitle := fmt.Sprintf("[kube-doctor][%s/%s] %s on %s/%s", criticality, severity, primaryAction.Operation, primaryAction.TargetKind, primaryAction.TargetName)
	mrDesc := buildPRDescription(issueType, criticality, severity, reason, primaryAction, followUpActions)
	mrPayload, _ := json.Marshal(map[string]string{
		"source_branch": branch,
		"target_branch": baseBranch,
		"title":         mrTitle,
		"description":   mrDesc,
	})
	mrReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fmt.Sprintf("%s/projects/%s/merge_requests", cfg.APIURL, encodedProject), bytes.NewReader(mrPayload))
	if err != nil {
		return "", 0, err
	}
	mrReq.Header.Set("Authorization", "Bearer "+cfg.AccessToken)
	mrReq.Header.Set("Content-Type", "application/json")
	mrResp, err := hc.Do(mrReq)
	if err != nil {
		return "", 0, err
	}
	defer mrResp.Body.Close()
	if mrResp.StatusCode >= 300 {
		raw, _ := io.ReadAll(mrResp.Body)
		return "", 0, fmt.Errorf("gitlab MR creation returned %d: %s", mrResp.StatusCode, string(raw))
	}
	var mrOut struct {
		IID    int    `json:"iid"`
		WebURL string `json:"web_url"`
	}
	if err := json.NewDecoder(mrResp.Body).Decode(&mrOut); err != nil {
		return "", 0, err
	}
	return mrOut.WebURL, mrOut.IID, nil
}

// HandleSCMWebhook processes GitHub pull_request merged webhooks and GitLab merge_request merged webhooks
// to automatically approve and execute the corresponding RemediationRequest in the cluster.
func (m *Manager) HandleSCMWebhook(
	ctx context.Context,
	provider string,
	payload []byte,
	executor func(ctx context.Context, act types.RemediationAction) error,
) (*types.ApprovalRequest, error) {
	p := strings.ToLower(strings.TrimSpace(provider))
	var branch, actor string
	var merged bool

	if p == "gitlab" {
		var glEvent struct {
			User struct {
				Username string `json:"username"`
			} `json:"user"`
			ObjectAttributes struct {
				SourceBranch string `json:"source_branch"`
				State        string `json:"state"`
				Action       string `json:"action"`
			} `json:"object_attributes"`
		}
		if err := json.Unmarshal(payload, &glEvent); err != nil {
			return nil, err
		}
		branch = glEvent.ObjectAttributes.SourceBranch
		actor = defaultStr(glEvent.User.Username, "gitlab-webhook")
		merged = strings.EqualFold(glEvent.ObjectAttributes.State, "merged") || strings.EqualFold(glEvent.ObjectAttributes.Action, "merge")
	} else {
		var ghEvent struct {
			Action      string `json:"action"`
			PullRequest struct {
				Merged bool `json:"merged"`
				Head   struct {
					Ref string `json:"ref"`
				} `json:"head"`
				MergedBy struct {
					Login string `json:"login"`
				} `json:"merged_by"`
			} `json:"pull_request"`
		}
		if err := json.Unmarshal(payload, &ghEvent); err != nil {
			return nil, err
		}
		branch = ghEvent.PullRequest.Head.Ref
		actor = defaultStr(ghEvent.PullRequest.MergedBy.Login, "github-webhook")
		merged = ghEvent.PullRequest.Merged || strings.EqualFold(ghEvent.Action, "merged")
	}

	if !merged || branch == "" {
		return nil, fmt.Errorf("webhook event is not a merged PR/MR (branch=%q, merged=%v)", branch, merged)
	}

	m.mu.RLock()
	var targetID string
	for _, req := range m.approvals {
		if req.GitOpsBranch == branch || strings.HasSuffix(branch, req.CRDName) {
			targetID = req.ID
			break
		}
	}
	m.mu.RUnlock()

	if targetID == "" {
		return nil, fmt.Errorf("no pending approval request matched branch %q", branch)
	}
	return m.ApproveAndExecute(ctx, targetID, actor, executor)
}

func (m *Manager) fetchGitHubUser(ctx context.Context, hc *http.Client, apiURL, token string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/user", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := hc.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var u struct {
		Login string `json:"login"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&u)
	return u.Login
}

func (m *Manager) fetchGitLabUser(ctx context.Context, hc *http.Client, apiURL, token string) string {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/user", nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := hc.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var u struct {
		Username string `json:"username"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&u)
	return u.Username
}

func buildPRDescription(
	issueType types.IssueType,
	criticality types.Criticality,
	severity types.Severity,
	reason string,
	primaryAction types.RemediationAction,
	followUpActions []types.RemediationAction,
) string {
	var b strings.Builder
	b.WriteString("## `kube-doctor` Autonomous SRE Approval Request\n\n")
	b.WriteString(fmt.Sprintf("- **Issue Type**: `%s`\n", issueType))
	b.WriteString(fmt.Sprintf("- **Criticality / Severity**: **`%s`** / **`%s`**\n", criticality, severity))
	b.WriteString(fmt.Sprintf("- **Target Resource**: `%s/%s` (Namespace: `%s`, APIVersion: `%s`)\n", primaryAction.TargetKind, primaryAction.TargetName, primaryAction.TargetNamespace, primaryAction.TargetAPIVersion))
	b.WriteString(fmt.Sprintf("- **Operation**: `%s`\n\n", primaryAction.Operation))
	b.WriteString("### Root Cause & Governance Reason\n")
	b.WriteString(reason + "\n\n")
	b.WriteString("### Primary Patch Payload\n```json\n" + primaryAction.PatchPayload + "\n```\n")
	if len(followUpActions) > 0 {
		b.WriteString("\n### Chained Follow-Up Actions (Executed Automatically After Approval)\n")
		for i, fa := range followUpActions {
			b.WriteString(fmt.Sprintf("%d. `%s` on `%s/%s`: %s\n", i+1, fa.Operation, fa.TargetKind, fa.TargetName, fa.Description))
		}
	}
	return b.String()
}
