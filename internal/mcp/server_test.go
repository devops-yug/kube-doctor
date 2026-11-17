package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/hub"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/watcher"
)

func TestMCPServerAndK8sMCPClient(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")
	t.Setenv("DISABLE_UPSTREAM_LLM", "true")

	kc := k8s.NewClient()
	gm := gitops.NewManager(kc)
	ragEng := rag.NewEngine("")
	k8sMCP := NewK8sMCPClient()
	remEng := remediation.NewEngine(kc, gm, k8sMCP)
	w := watcher.NewWatcher(kc, remEng)
	hm := hub.NewManager("hub", kc, remEng, w)

	srv := NewServer(ragEng, remEng, gm, k8sMCP)
	srv.SetWatcher(w)
	srv.SetHubAndK8sClient(hm, kc)

	// 1. Test HTTP method branches (DELETE, GET, PUT, invalid JSON, notification)
	delRec := httptest.NewRecorder()
	srv.ServeHTTP(delRec, httptest.NewRequest(http.MethodDelete, "/mcp", nil))
	if delRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on DELETE, got %d", delRec.Code)
	}

	getRec := httptest.NewRecorder()
	srv.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/mcp", nil))
	if getRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET, got %d", getRec.Code)
	}

	putRec := httptest.NewRecorder()
	srv.ServeHTTP(putRec, httptest.NewRequest(http.MethodPut, "/mcp", nil))
	if putRec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on PUT, got %d", putRec.Code)
	}

	badRec := httptest.NewRecorder()
	srv.ServeHTTP(badRec, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader("{bad")))
	if !strings.Contains(badRec.Body.String(), "-32700") {
		t.Fatalf("expected parse error -32700, got %s", badRec.Body.String())
	}

	notifRec := httptest.NewRecorder()
	srv.ServeHTTP(notifRec, httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)))
	if notifRec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for notification, got %d", notifRec.Code)
	}

	// 2. Test MCP JSON-RPC methods
	rpcMethods := []string{
		"initialize",
		"server/discover",
		"ping",
		"tools/list",
		"prompts/list",
		"resources/list",
		"resources/templates/list",
		"logging/setLevel",
		"unknown/method",
	}
	for _, m := range rpcMethods {
		body, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  m,
			"params":  map[string]any{"protocolVersion": "2026-07-28"},
		})
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("method %s returned HTTP %d", m, rec.Code)
		}
	}

	// 3. Test all MCP tools via tools/call
	toolCalls := []struct {
		name string
		args map[string]any
	}{
		{"scan_and_remediate_cluster_health", map[string]any{"forceReevaluate": true}},
		{"list_active_incidents", map[string]any{"includeResolved": true}},
		{"list_managed_clusters", map[string]any{}},
		{"inspect_resource_hierarchy", map[string]any{"name": "pg-cluster-1"}},
		{"list_operator_adapters", map[string]any{}},
		{"verify_remediation_effectiveness", map[string]any{}},
		{"diagnose_and_remediate_cluster_issue", map[string]any{
			"namespace":        "prod",
			"podName":          "db-0",
			"reason":           "OOMKilled",
			"exitCode":         137,
			"quotaMemoryLimit": "512Mi",
			"quotaMemoryUsed":  "512Mi",
		}},
		{"query_k8s_135_ocp_422_knowledge", map[string]any{"query": "pods/resize 1.35"}},
		{ToolEventsList, map[string]any{"namespace": "prod"}},
		{ToolPodsGet, map[string]any{"namespace": "prod", "name": "db-0"}},
		{ToolNodesStatsSummary, map[string]any{"name": "node-1"}},
	}

	for _, tc := range toolCalls {
		body, _ := json.Marshal(map[string]any{
			"jsonrpc": "2.0",
			"id":      2,
			"method":  "tools/call",
			"params": map[string]any{
				"name":      tc.name,
				"arguments": tc.args,
			},
		})
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(body)))
		if strings.Contains(rec.Body.String(), `"error"`) {
			t.Fatalf("tool %s returned error: %s", tc.name, rec.Body.String())
		}
	}

	// Test approve_remediation_request tool on the approval created above
	approvals := gm.ListApprovals()
	if len(approvals) == 0 {
		t.Fatal("expected at least 1 pending approval")
	}
	appBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      3,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      "approve_remediation_request",
			"arguments": map[string]any{"approvalId": approvals[0].ID},
		},
	})
	appRec := httptest.NewRecorder()
	srv.ServeHTTP(appRec, httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(appBody)))
	if strings.Contains(appRec.Body.String(), `"error"`) {
		t.Fatalf("approve_remediation_request failed: %s", appRec.Body.String())
	}

	// Test unknown tool error
	unkBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      4,
		"method":  "tools/call",
		"params":  map[string]any{"name": "unknown_tool_xyz"},
	})
	unkRec := httptest.NewRecorder()
	srv.ServeHTTP(unkRec, httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(unkBody)))
	if !strings.Contains(unkRec.Body.String(), "unknown tool") {
		t.Fatalf("expected unknown tool error, got %s", unkRec.Body.String())
	}

	// 4. Test K8sMCPClient Streamable HTTP & Stdio transports and helper methods
	ctx := context.Background()
	_, _ = k8sMCP.GetPodLogs(ctx, "prod", "db-0", "postgres", true, 50)
	_, _ = k8sMCP.ListEvents(ctx, "prod", "involvedObject.name=db-0")
	_, _ = k8sMCP.GetResource(ctx, "apps/v1", "StatefulSet", "prod", "db")
	_, _ = k8sMCP.CreateOrUpdateResource(ctx, `{"kind":"Pod"}`)
	_, _ = k8sMCP.ScaleResource(ctx, "apps/v1", "StatefulSet", "prod", "db", 3)
	if len(k8sMCP.CallRecords()) == 0 {
		t.Fatal("expected recorded MCP calls")
	}

	// Streamable HTTP server test (including SSE framing & error response)
	mockSidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Mcp-Session-Id", "sess-123")
		_, _ = w.Write([]byte("event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"live-sidecar-ok\"}]}}\n\n"))
	}))
	defer mockSidecar.Close()

	k8sMCP.SetServerURL(mockSidecar.URL)
	out, err := k8sMCP.GetPod(ctx, "prod", "pod-1")
	if err != nil || out != "live-sidecar-ok" {
		t.Fatalf("expected live-sidecar-ok, got %q (%v)", out, err)
	}

	// Test parseMCPToolResponse error & raw branches
	if _, err := parseMCPToolResponse([]byte(`{"error":{"code":-32000,"message":"boom"}}`)); err == nil {
		t.Fatal("expected MCP tool error")
	}
	if rawOut, err := parseMCPToolResponse([]byte(`not-json`)); err != nil || rawOut != "not-json" {
		t.Fatalf("expected raw fallback, got %q (%v)", rawOut, err)
	}

	// Test callStdio using `cat` or `sh -c` echo
	t.Setenv("KUBE_SIMULATION_MODE", "false")
	t.Setenv("K8S_MCP_SERVER_URL", "")
	t.Setenv("K8S_MCP_SERVER_COMMAND", `sh -c "read line1; echo '{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}'; read line2; echo '{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"stdio-ok\"}]}}'"`)
	stdioClient := NewK8sMCPClient()
	stdioClient.command = "sh"
	stdioClient.args = []string{"-c", `read line1; echo '{"jsonrpc":"2.0","id":1,"result":{}}'; read line2; echo '{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"stdio-ok"}]}}'`}
	stdioClient.simulate = false
	stdioOut, err := stdioClient.GetPod(ctx, "default", "p1")
	if err != nil || stdioOut != "stdio-ok" {
		t.Fatalf("stdio client failed: %q (%v)", stdioOut, err)
	}

	// Test NewServer without optional k8sMCP argument & without hubManager
	srvNoHub := NewServer(ragEng, remEng, gm)
	srvNoHub.SetWatcher(w)
	incBody, _ := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      5,
		"method":  "tools/call",
		"params":  map[string]any{"name": "list_active_incidents", "arguments": map[string]any{}},
	})
	incRec := httptest.NewRecorder()
	srvNoHub.ServeHTTP(incRec, httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewReader(incBody)))
	if incRec.Code != http.StatusOK {
		t.Fatalf("list_active_incidents without hubManager failed: %d", incRec.Code)
	}
	simClient := NewK8sMCPClient()
	simClient.simulate = true
	_, _ = simClient.CallTool(ctx, "unknown_sim_tool", map[string]any{"namespace": "default", "name": "x"})
	_, _ = simClient.CallTool(ctx, "pods_get", map[string]any{"name": "noobaa-core-0"})
}
