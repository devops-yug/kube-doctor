package gitops

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

func TestGitOpsManagerApprovalsAndOAuth(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	t.Setenv("GITHUB_OAUTH_TOKEN", "gho_initial_token")
	t.Setenv("GITLAB_OAUTH_TOKEN", "glo_initial_token")
	t.Setenv("KUBE_DOCTOR_INCLUSTER_APPROVAL", "false")

	kc := k8s.NewClient()
	mgr := NewManager(kc)

	st := mgr.GetIntegrationStatus()
	if !st.GitHub.Connected || !st.GitLab.Connected || st.InClusterAllowed {
		t.Fatalf("unexpected initial status: %+v", st)
	}

	// Test SetApprovalMode decision branches
	mgr.SetApprovalMode("IN_CLUSTER", true)
	if mgr.GetIntegrationStatus().ApprovalMode != ApprovalModeInCluster || !mgr.GetIntegrationStatus().InClusterAllowed {
		t.Fatal("expected IN_CLUSTER mode")
	}
	mgr.SetApprovalMode("invalid-mode", true)
	if mgr.GetIntegrationStatus().ApprovalMode != ApprovalModeHybrid {
		t.Fatal("expected fallback to HYBRID mode")
	}

	// Set up mock GitHub & GitLab OAuth + REST API server
	mux := http.NewServeMux()
	mux.HandleFunc("/login/oauth/access_token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "gho_live_token"})
	})
	mux.HandleFunc("/user", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"login": "octocat-sre"})
	})
	mux.HandleFunc("/repos/org/openshift-gitops-fleet/git/ref/heads/main", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"object": map[string]string{"sha": "abc12345"}})
	})
	mux.HandleFunc("/repos/org/openshift-gitops-fleet/git/refs", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ref":"refs/heads/branch"}`))
	})
	mux.HandleFunc("/repos/org/openshift-gitops-fleet/contents/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})
	mux.HandleFunc("/repos/org/openshift-gitops-fleet/pulls", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"number":   42,
			"html_url": "https://github.com/org/openshift-gitops-fleet/pull/42",
		})
	})
	// GitLab endpoints
	mux.HandleFunc("/oauth/token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"access_token":  "glo_live_token",
			"refresh_token": "glo_refresh",
		})
	})
	mux.HandleFunc("/api/v4/user", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"username": "gitlab-sre"})
	})
	mux.HandleFunc("/api/v4/projects/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/merge_requests") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"iid":     77,
				"web_url": "https://gitlab.com/org/openshift-gitops-fleet/-/merge_requests/77",
			})
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{}`))
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()
	mgr.SetHTTPClient(srv.Client())

	// Configure providers pointing to mock server
	if _, err := mgr.ConfigureProvider(OAuthProviderConfig{
		Provider:     "github",
		ClientID:     "gh-cid",
		ClientSecret: "gh-sec",
		RedirectURI:  "http://localhost:8080/cb",
		WebURL:       srv.URL,
		APIURL:       srv.URL,
		Repository:   "org/openshift-gitops-fleet",
		BaseBranch:   "main",
		AccessToken:  "gho_live_token",
		ConnectedUser: "octocat",
	}); err != nil {
		t.Fatalf("ConfigureProvider github failed: %v", err)
	}
	if _, err := mgr.ConfigureProvider(OAuthProviderConfig{
		Provider:     "gitlab",
		ClientID:     "gl-cid",
		ClientSecret: "gl-sec",
		RedirectURI:  "http://localhost:8080/cb",
		WebURL:       srv.URL,
		Repository:   "org/openshift-gitops-fleet",
		BaseBranch:   "main",
		AccessToken:  "glo_live_token",
	}); err != nil {
		t.Fatalf("ConfigureProvider gitlab failed: %v", err)
	}
	if _, err := mgr.ConfigureProvider(OAuthProviderConfig{Provider: "unknown"}); err == nil {
		t.Fatal("expected error for unknown provider")
	}

	// Test OAuth Authorize URL & ExchangeOAuthCode (both real HTTP & simulated)
	ghAuthURL, ghState, err := mgr.GetOAuthAuthorizeURL("github", "http://localhost/cb")
	if err != nil || !strings.Contains(ghAuthURL, "client_id=gh-cid") {
		t.Fatalf("unexpected ghAuthURL: %s (%v)", ghAuthURL, err)
	}
	glAuthURL, glState, err := mgr.GetOAuthAuthorizeURL("gitlab", "http://localhost/cb")
	if err != nil || !strings.Contains(glAuthURL, "client_id=gl-cid") {
		t.Fatalf("unexpected glAuthURL: %s (%v)", glAuthURL, err)
	}
	if _, _, err := mgr.GetOAuthAuthorizeURL("invalid", ""); err == nil {
		t.Fatal("expected error for invalid oauth provider")
	}

	// State mismatch error
	if _, err := mgr.ExchangeOAuthCode(context.Background(), "gitlab", "code123", ghState); err == nil {
		t.Fatal("expected state provider mismatch error")
	}
	if _, err := mgr.ExchangeOAuthCode(context.Background(), "github", "", ""); err == nil {
		t.Fatal("expected empty code error")
	}
	if _, err := mgr.ExchangeOAuthCode(context.Background(), "invalid", "code", ""); err == nil {
		t.Fatal("expected invalid provider error")
	}

	// Live GitHub & GitLab OAuth exchange
	ghSt, err := mgr.ExchangeOAuthCode(context.Background(), "github", "live-code", "")
	if err != nil || !ghSt.Connected || ghSt.ConnectedUser != "octocat-sre" {
		t.Fatalf("github ExchangeOAuthCode failed: %+v, %v", ghSt, err)
	}
	glSt, err := mgr.ExchangeOAuthCode(context.Background(), "gitlab", "live-code", glState)
	if err != nil || !glSt.Connected || glSt.ConnectedUser != "gitlab-sre" {
		t.Fatalf("gitlab ExchangeOAuthCode failed: %+v, %v", glSt, err)
	}

	// Simulated OAuth exchange
	simSt, err := mgr.ExchangeOAuthCode(context.Background(), "gitlab", "sim-test", "")
	if err != nil || !simSt.Connected {
		t.Fatalf("sim ExchangeOAuthCode failed: %v", err)
	}
	// Restore live token on gitlab
	_, _ = mgr.ExchangeOAuthCode(context.Background(), "gitlab", "live-code", "")

	// CreateApprovalRequest with GitHub PR
	mgr.SetApprovalMode(ApprovalModeGitHub, true)
	req1, err := mgr.CreateApprovalRequest(
		context.Background(),
		types.IssueNamespaceQuotaExceed,
		types.CriticalityP0Critical,
		types.SeveritySev1,
		"Quota exceeded",
		types.RemediationAction{
			ActionID:         "act-1",
			TargetAPIVersion: "v1",
			TargetKind:       "ResourceQuota",
			TargetNamespace:  "prod",
			TargetName:       "Quota_One!",
			Operation:        "UPDATE_QUOTA",
			PatchPayload:     `{"spec":{"hard":{"limits.memory":"4Gi"}}}`,
		},
		[]types.RemediationAction{
			{ActionID: "act-2", TargetKind: "StatefulSet", TargetName: "db", Operation: "PATCH_STS"},
		},
	)
	if err != nil || req1.GitOpsPRNumber != 42 {
		t.Fatalf("expected PR #42 from GitHub, got %+v (%v)", req1, err)
	}

	// CreateSCMRequestForApproval for GitLab MR
	updatedReq, err := mgr.CreateSCMRequestForApproval(context.Background(), req1.ID, "gitlab")
	if err != nil || updatedReq.GitOpsPRNumber != 77 {
		t.Fatalf("expected MR !77 from GitLab, got %+v (%v)", updatedReq, err)
	}
	// Also test CreateSCMRequestForApproval with github and invalid
	if _, err := mgr.CreateSCMRequestForApproval(context.Background(), req1.ID, ""); err != nil {
		t.Fatalf("CreateSCMRequestForApproval default provider failed: %v", err)
	}
	if _, err := mgr.CreateSCMRequestForApproval(context.Background(), "not-found", "github"); err == nil {
		t.Fatal("expected error for unknown approval ID")
	}
	if _, err := mgr.CreateSCMRequestForApproval(context.Background(), req1.ID, "bitbucket"); err == nil {
		t.Fatal("expected error for unsupported provider")
	}

	// Lookup by prefix, CRDName, ActionID
	if _, ok := mgr.GetApproval(req1.ID[:8]); !ok {
		t.Fatal("expected prefix lookup to succeed")
	}
	if _, ok := mgr.GetApproval(req1.CRDName); !ok {
		t.Fatal("expected CRDName lookup to succeed")
	}
	if _, ok := mgr.GetApproval("nonexistent"); ok {
		t.Fatal("expected nonexistent lookup to fail")
	}
	if len(mgr.ListApprovals()) != 1 {
		t.Fatalf("expected 1 approval, got %d", len(mgr.ListApprovals()))
	}

	// Test GitHub & GitLab SCM Webhooks
	ghWebhook := map[string]any{
		"action": "closed",
		"pull_request": map[string]any{
			"merged":    true,
			"head":      map[string]string{"ref": req1.GitOpsBranch},
			"merged_by": map[string]string{"login": "gh-merger"},
		},
	}
	ghBytes, _ := json.Marshal(ghWebhook)
	approvedReq, err := mgr.HandleSCMWebhook(context.Background(), "github", ghBytes, func(ctx context.Context, act types.RemediationAction) error {
		return nil
	})
	if err != nil || approvedReq.Status != "APPROVED" || approvedReq.ApprovedBy != "gh-merger" {
		t.Fatalf("HandleSCMWebhook github failed: %+v (%v)", approvedReq, err)
	}
	// Calling ApproveAndExecute again on an already-approved request returns immediately
	if _, err := mgr.ApproveAndExecute(context.Background(), req1.ID, "admin", nil); err != nil {
		t.Fatalf("idempotent ApproveAndExecute failed: %v", err)
	}

	// Test GitLab mode CreateApprovalRequest + GitLab Webhook + Reject
	mgr.SetApprovalMode(ApprovalModeGitLab, true)
	_ = mgr.DisconnectProvider("github")
	req2, err := mgr.CreateApprovalRequest(
		context.Background(),
		types.IssueMachineMissing,
		types.CriticalityP0Critical,
		types.SeveritySev1,
		"Machine missing",
		types.RemediationAction{
			ActionID:         "act-ms",
			TargetAPIVersion: "machine.openshift.io/v1beta1",
			TargetKind:       "MachineSet",
			TargetName:       "",
			Operation:        "SCALE_MS",
			PatchPayload:     `{"spec":{"replicas":3}}`,
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Reject(req2.ID, "sec-admin"); err != nil {
		t.Fatalf("Reject failed: %v", err)
	}
	if _, err := mgr.Reject("unknown-id", "sec-admin"); err == nil {
		t.Fatal("expected error rejecting unknown ID")
	}

	// Test GitLab webhook merge on a new request
	req3, _ := mgr.CreateApprovalRequest(context.Background(), types.IssueMachineMissing, types.CriticalityP0Critical, types.SeveritySev1, "test", types.RemediationAction{TargetKind: "MachineSet", TargetName: "w1"}, nil)
	glWebhook := map[string]any{
		"user": map[string]string{"username": "gl-merger"},
		"object_attributes": map[string]string{
			"source_branch": req3.GitOpsBranch,
			"state":         "merged",
		},
	}
	glBytes, _ := json.Marshal(glWebhook)
	if _, err := mgr.HandleSCMWebhook(context.Background(), "gitlab", glBytes, nil); err != nil {
		t.Fatalf("HandleSCMWebhook gitlab failed: %v", err)
	}
	if _, err := mgr.HandleSCMWebhook(context.Background(), "gitlab", []byte(`{"object_attributes":{"state":"opened"}}`), nil); err == nil {
		t.Fatal("expected error for unmerged MR webhook")
	}

	// Disconnect providers
	if err := mgr.DisconnectProvider("gitlab"); err != nil {
		t.Fatal(err)
	}
	if err := mgr.DisconnectProvider("invalid"); err == nil {
		t.Fatal("expected error for invalid provider disconnect")
	}
}
