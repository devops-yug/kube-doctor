package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/mcp"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
)

func TestLLMEngineChatAndDiagnose(t *testing.T) {
	t.Setenv("KUBE_SIMULATION_MODE", "true")

	// Start mock upstream LLM server
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer fail-token" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(types.ChatCompletionResponse{
			ID:     "up-1",
			Object: "chat.completion",
			Model:  DefaultModelName,
			Choices: []types.ChatCompletionChoice{
				{
					Index:        0,
					Message:      types.ChatMessage{Role: "assistant", Content: "Upstream LLM root cause analysis"},
					FinishReason: "stop",
				},
			},
		})
	}))
	defer upSrv.Close()

	t.Setenv("UPSTREAM_LLM_BASE_URL", upSrv.URL)
	t.Setenv("UPSTREAM_LLM_API_KEY", "test-key")

	ragEng := rag.NewEngine("")
	kc := k8s.NewClient()
	gm := gitops.NewManager(kc)
	remEng := remediation.NewEngine(kc, gm)
	eng := NewEngine(ragEng, remEng, kc)

	ctx := context.Background()

	// 1. DiagnoseAndRemediateEvent with upstream LLM
	assess, reasoning, err := eng.DiagnoseAndRemediateEvent(ctx, types.DiagnosticEvent{
		Namespace:     "prod",
		PodName:       "db-0",
		ContainerName: "postgres",
		OwnerKind:     "StatefulSet",
		OwnerName:     "db",
		ExitCode:      137,
		Reason:        "OOMKilled",
		Message:       strings.Repeat("long message ", 60),
	})
	if err != nil || assess == nil || reasoning != "Upstream LLM root cause analysis" {
		t.Fatalf("DiagnoseAndRemediateEvent failed: %v, reasoning=%q", err, reasoning)
	}

	// 2. DiagnoseAndRemediateEvent fallback when upstream fails
	eng.upstreamAPIKey = "fail-token"
	assess2, reasoning2, err := eng.DiagnoseAndRemediateEvent(ctx, types.DiagnosticEvent{
		Namespace:       "prod",
		PodName:         "pg-cluster-1",
		OwnerAPIVersion: "postgresql.cnpg.io/v1",
		OwnerKind:       "Cluster",
		OwnerName:       "pg-cluster",
		ExitCode:        137,
		Reason:          "OOMKilled",
		OwnershipChain: []types.OwnerChainLink{
			{Kind: "Pod", Name: "pg-cluster-1", APIVersion: "v1"},
			{Kind: "Cluster", Name: "pg-cluster", APIVersion: "postgresql.cnpg.io/v1"},
		},
	})
	if err != nil || assess2 == nil || !strings.Contains(reasoning2, "Specialist Assessment") {
		t.Fatalf("expected specialist fallback response, got: %v, %q", err, reasoning2)
	}

	// 3. ChatCompletion requesting external MCP + Web tools (pods_get + resources_get + web_search)
	toolResp1, err := eng.ChatCompletion(ctx, types.ChatCompletionRequest{
		Messages: []types.ChatMessage{{Role: "user", Content: "Please search web for CVE-2026-1111 and fetch mcp pod status"}},
		Tools: []types.ChatTool{
			{Type: "function", Function: types.ChatToolFunction{Name: mcp.ToolPodsGet}},
			{Type: "function", Function: types.ChatToolFunction{Name: mcp.ToolResourcesGet}},
			{Type: "function", Function: types.ChatToolFunction{Name: "web_search"}},
		},
	})
	if err != nil || toolResp1.Choices[0].FinishReason != "tool_calls" || len(toolResp1.Choices[0].Message.ToolCalls) < 2 {
		t.Fatalf("expected tool_calls for pods_get + web_search, got %+v (%v)", toolResp1, err)
	}

	// 4. ChatCompletion requesting pods_log
	toolResp2, err := eng.ChatCompletion(ctx, types.ChatCompletionRequest{
		Messages: []types.ChatMessage{{Role: "user", Content: "check logs from mcp"}},
		Tools: []types.ChatTool{
			{Type: "function", Function: types.ChatToolFunction{Name: mcp.ToolPodsLog}},
		},
	})
	if err != nil || toolResp2.Choices[0].Message.ToolCalls[0].Function.Name != mcp.ToolPodsLog {
		t.Fatalf("expected pods_log tool call, got %+v", toolResp2)
	}

	// 5. ChatCompletion requesting default MCP tool
	toolResp3, err := eng.ChatCompletion(ctx, types.ChatCompletionRequest{
		Messages: []types.ChatMessage{{Role: "user", Content: "ask mcp for prometheus metrics"}},
		Tools: []types.ChatTool{
			{Type: "function", Function: types.ChatToolFunction{Name: "prometheus_mcp_query"}},
		},
	})
	if err != nil || toolResp3.Choices[0].Message.ToolCalls[0].Function.Name != "prometheus_mcp_query" {
		t.Fatalf("expected prometheus_mcp_query tool call, got %+v", toolResp3)
	}

	// 6. ChatCompletion with tool result and various prompt inferences (CNPG, NooBaa, MachineSet, OOM+Quota, keyword)
	prompts := []string{
		"cnpg pg-cluster disk wal full exceeding quota",
		"cnpg pg-cluster oom",
		"noobaa backingstore degraded",
		"machineset machine missing",
		"statefulset oom 137 resourcequota exceeded",
		"coredns servfail",
	}
	for _, p := range prompts {
		resp, err := eng.ChatCompletion(ctx, types.ChatCompletionRequest{
			Messages: []types.ChatMessage{
				{Role: "user", Content: p},
				{Role: "tool", ToolCallID: "call-1", Content: "pod status output"},
			},
		})
		if err != nil || len(resp.Choices) == 0 {
			t.Fatalf("ChatCompletion(%q) failed: %v", p, err)
		}
	}

	// 7. ChatCompletion with explicit DiagnosticEvent and working upstream LLM
	eng.upstreamAPIKey = "valid-key"
	upResp, err := eng.ChatCompletion(ctx, types.ChatCompletionRequest{
		Messages:        []types.ChatMessage{{Role: "user", Content: "diagnose"}},
		DiagnosticEvent: &types.DiagnosticEvent{Reason: "OOMKilled", ExitCode: 137},
	})
	if err != nil || upResp.TriageAssessment == nil {
		t.Fatalf("ChatCompletion with DiagnosticEvent failed: %v", err)
	}
}
