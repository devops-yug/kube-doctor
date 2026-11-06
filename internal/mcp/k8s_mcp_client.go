package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// KubernetesMCPToolNames defines the canonical tool names exposed by
// https://github.com/containers/kubernetes-mcp-server
const (
	ToolEventsList              = "events_list"
	ToolNamespacesList          = "namespaces_list"
	ToolProjectsList            = "projects_list"
	ToolNodesLog                = "nodes_log"
	ToolNodesStatsSummary       = "nodes_stats_summary"
	ToolNodesTop                = "nodes_top"
	ToolPodsList                = "pods_list"
	ToolPodsListInNamespace     = "pods_list_in_namespace"
	ToolPodsGet                 = "pods_get"
	ToolPodsDelete              = "pods_delete"
	ToolPodsTop                 = "pods_top"
	ToolPodsExec                = "pods_exec"
	ToolPodsLog                 = "pods_log"
	ToolResourcesList           = "resources_list"
	ToolResourcesGet            = "resources_get"
	ToolResourcesCreateOrUpdate = "resources_create_or_update"
	ToolResourcesDelete         = "resources_delete"
	ToolResourcesScale          = "resources_scale"
)

// K8sMCPClient connects to an existing github.com/containers/kubernetes-mcp-server instance
// via Streamable HTTP (e.g., http://127.0.0.1:8081/mcp) or Stdio subprocess
// (e.g., "kubernetes-mcp-server" or "npx -y kubernetes-mcp-server@latest").
type K8sMCPClient struct {
	serverURL  string
	command    string
	args       []string
	httpClient *http.Client
	sessionID  string
	reqID      atomic.Int64

	mu       sync.Mutex
	callLog  []MCPCallRecord
	simulate bool
}

// MCPCallRecord tracks every MCP tool invocation sent to containers/kubernetes-mcp-server.
type MCPCallRecord struct {
	Timestamp time.Time      `json:"timestamp"`
	Transport string         `json:"transport"` // "streamable-http", "stdio", or "simulated-mcp"
	ToolName  string         `json:"toolName"`
	Arguments map[string]any `json:"arguments"`
	Response  string         `json:"response"`
}

// NewK8sMCPClient creates a client for github.com/containers/kubernetes-mcp-server.
// Configured via:
//   - K8S_MCP_SERVER_URL (e.g. "http://127.0.0.1:8081/mcp") for Streamable HTTP mode
//   - K8S_MCP_SERVER_COMMAND (e.g. "kubernetes-mcp-server" or "npx -y kubernetes-mcp-server@latest") for Stdio mode
func NewK8sMCPClient() *K8sMCPClient {
	serverURL := strings.TrimSpace(os.Getenv("K8S_MCP_SERVER_URL"))
	cmdStr := strings.TrimSpace(os.Getenv("K8S_MCP_SERVER_COMMAND"))
	var cmd string
	var args []string
	if cmdStr != "" {
		parts := strings.Fields(cmdStr)
		cmd = parts[0]
		if len(parts) > 1 {
			args = parts[1:]
		}
	}

	simulate := (serverURL == "" && cmd == "") || os.Getenv("KUBE_SIMULATION_MODE") == "true"

	return &K8sMCPClient{
		serverURL: serverURL,
		command:   cmd,
		args:      args,
		simulate:  simulate,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

// SetServerURL configures the Streamable HTTP endpoint of containers/kubernetes-mcp-server dynamically (e.g. in tests).
func (c *K8sMCPClient) SetServerURL(url string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.serverURL = strings.TrimSpace(url)
	c.simulate = c.serverURL == "" && c.command == ""
}

// CallTool invokes a tool on containers/kubernetes-mcp-server using MCP JSON-RPC 2.0 (tools/call).
func (c *K8sMCPClient) CallTool(ctx context.Context, toolName string, arguments map[string]any) (string, error) {
	if c.simulate {
		resp := c.simulateToolResponse(toolName, arguments)
		c.recordCall("simulated-mcp", toolName, arguments, resp)
		return resp, nil
	}

	if c.serverURL != "" {
		resp, err := c.callStreamableHTTP(ctx, toolName, arguments)
		if err != nil {
			return "", err
		}
		c.recordCall("streamable-http", toolName, arguments, resp)
		return resp, nil
	}

	resp, err := c.callStdio(ctx, toolName, arguments)
	if err != nil {
		return "", err
	}
	c.recordCall("stdio", toolName, arguments, resp)
	return resp, nil
}

// High-level helper methods wrapping github.com/containers/kubernetes-mcp-server tools:

// GetPod calls `pods_get` on containers/kubernetes-mcp-server.
func (c *K8sMCPClient) GetPod(ctx context.Context, namespace, name string) (string, error) {
	return c.CallTool(ctx, ToolPodsGet, map[string]any{
		"namespace": namespace,
		"name":      name,
	})
}

// GetPodLogs calls `pods_log` on containers/kubernetes-mcp-server (with previous=true for OOMKilled/crashed pods).
func (c *K8sMCPClient) GetPodLogs(ctx context.Context, namespace, name, container string, previous bool, tail int) (string, error) {
	args := map[string]any{
		"namespace": namespace,
		"name":      name,
		"previous":  previous,
		"tail":      tail,
	}
	if container != "" {
		args["container"] = container
	}
	return c.CallTool(ctx, ToolPodsLog, args)
}

// GetNodeStatsSummary calls `nodes_stats_summary` on containers/kubernetes-mcp-server
// to retrieve kubelet Summary API + cgroup v2 PSI (Pressure Stall Information) metrics.
func (c *K8sMCPClient) GetNodeStatsSummary(ctx context.Context, nodeName string) (string, error) {
	return c.CallTool(ctx, ToolNodesStatsSummary, map[string]any{
		"name": nodeName,
	})
}

// ListEvents calls `events_list` on containers/kubernetes-mcp-server.
func (c *K8sMCPClient) ListEvents(ctx context.Context, namespace, fieldSelector string) (string, error) {
	args := map[string]any{}
	if namespace != "" {
		args["namespace"] = namespace
	}
	if fieldSelector != "" {
		args["fieldSelector"] = fieldSelector
	}
	return c.CallTool(ctx, ToolEventsList, args)
}

// GetResource calls `resources_get` on containers/kubernetes-mcp-server for any K8s/OpenShift GVK.
func (c *K8sMCPClient) GetResource(ctx context.Context, apiVersion, kind, namespace, name string) (string, error) {
	args := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"name":       name,
	}
	if namespace != "" {
		args["namespace"] = namespace
	}
	return c.CallTool(ctx, ToolResourcesGet, args)
}

// CreateOrUpdateResource calls `resources_create_or_update` on containers/kubernetes-mcp-server
// using Server-Side Apply with a complete YAML/JSON resource manifest.
func (c *K8sMCPClient) CreateOrUpdateResource(ctx context.Context, resourceYAMLOrJSON string) (string, error) {
	return c.CallTool(ctx, ToolResourcesCreateOrUpdate, map[string]any{
		"resource": resourceYAMLOrJSON,
	})
}

// ScaleResource calls `resources_scale` on containers/kubernetes-mcp-server
// (supports StatefulSet, Deployment, and OpenShift MachineSet).
func (c *K8sMCPClient) ScaleResource(ctx context.Context, apiVersion, kind, namespace, name string, scale int) (string, error) {
	args := map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"name":       name,
		"scale":      scale,
	}
	if namespace != "" {
		args["namespace"] = namespace
	}
	return c.CallTool(ctx, ToolResourcesScale, args)
}

// CallRecords returns the log of all calls made to containers/kubernetes-mcp-server.
func (c *K8sMCPClient) CallRecords() []MCPCallRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]MCPCallRecord, len(c.callLog))
	copy(out, c.callLog)
	return out
}

func (c *K8sMCPClient) callStreamableHTTP(ctx context.Context, toolName string, arguments map[string]any) (string, error) {
	id := c.reqID.Add(1)
	rpcReq := map[string]any{
		"jsonrpc": "2.0",
		"id":      id,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": arguments,
		},
	}
	bodyBytes, err := json.Marshal(rpcReq)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.serverURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if c.sessionID != "" {
		req.Header.Set("Mcp-Session-Id", c.sessionID)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if sid := resp.Header.Get("Mcp-Session-Id"); sid != "" {
		c.sessionID = sid
	}

	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("kubernetes-mcp-server HTTP %d: %s", resp.StatusCode, string(rawBytes))
	}

	return parseMCPToolResponse(rawBytes)
}

func (c *K8sMCPClient) callStdio(ctx context.Context, toolName string, arguments map[string]any) (string, error) {
	cmd := exec.CommandContext(ctx, c.command, c.args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
	}()

	initReq := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"kube-doctor","version":"1.35.0"}}}` + "\n"
	_, _ = io.WriteString(stdin, initReq)

	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, 1024*1024), 4*1024*1024)
	_ = scanner.Scan() // consume initialize response

	callObj := map[string]any{
		"jsonrpc": "2.0",
		"id":      2,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      toolName,
			"arguments": arguments,
		},
	}
	callBytes, _ := json.Marshal(callObj)
	_, _ = io.WriteString(stdin, string(callBytes)+"\n")

	if !scanner.Scan() {
		return "", fmt.Errorf("no response from stdio kubernetes-mcp-server for tool %s", toolName)
	}
	return parseMCPToolResponse(scanner.Bytes())
}

func parseMCPToolResponse(raw []byte) (string, error) {
	// Support both standard JSON-RPC response and SSE-framed Streamable HTTP response
	payload := strings.TrimSpace(string(raw))
	for _, line := range strings.Split(payload, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "data:") {
			payload = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			break
		}
	}

	var parsed struct {
		Result struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(payload), &parsed); err != nil {
		return payload, nil
	}
	if parsed.Error != nil {
		return "", fmt.Errorf("MCP tool error (%d): %s", parsed.Error.Code, parsed.Error.Message)
	}
	var texts []string
	for _, item := range parsed.Result.Content {
		if item.Text != "" {
			texts = append(texts, item.Text)
		}
	}
	if len(texts) > 0 {
		return strings.Join(texts, "\n"), nil
	}
	return payload, nil
}

func (c *K8sMCPClient) simulateToolResponse(toolName string, args map[string]any) string {
	switch toolName {
	case ToolResourcesCreateOrUpdate:
		return fmt.Sprintf("kubernetes-mcp-server: resources_create_or_update applied via Server-Side Apply (%d bytes)", len(fmt.Sprint(args["resource"])))
	case ToolResourcesScale:
		return fmt.Sprintf("kubernetes-mcp-server: resources_scale updated %v/%v (%v) to scale=%v", args["kind"], args["name"], args["apiVersion"], args["scale"])
	case ToolPodsGet:
		return fmt.Sprintf("kubernetes-mcp-server: Pod %v/%v status=OOMKilled exitCode=137", args["namespace"], args["name"])
	case ToolNodesStatsSummary:
		return fmt.Sprintf("kubernetes-mcp-server: Node %v cgroup v2 PSI memory.pressure some avg10=18.40 full avg10=6.20", args["name"])
	default:
		b, _ := json.Marshal(args)
		return fmt.Sprintf("kubernetes-mcp-server: executed %s(%s)", toolName, string(b))
	}
}

func (c *K8sMCPClient) recordCall(transport, toolName string, args map[string]any, resp string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.callLog = append(c.callLog, MCPCallRecord{
		Timestamp: time.Now().UTC(),
		Transport: transport,
		ToolName:  toolName,
		Arguments: args,
		Response:  resp,
	})
}

// ExportedToolSchemas returns the OpenAI function tool definitions for
// github.com/containers/kubernetes-mcp-server so any LLM or Copilot agent can call them.
func ExportedToolSchemas() []map[string]any {
	return []map[string]any{
		{
			"name":        ToolEventsList,
			"description": "List Kubernetes events (warnings, errors, state changes) via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"namespace":     map[string]any{"type": "string"},
					"fieldSelector": map[string]any{"type": "string"},
				},
			},
		},
		{
			"name":        ToolNamespacesList,
			"description": "List all Kubernetes Namespaces via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type":       "object",
				"properties": map[string]any{},
			},
		},
		{
			"name":        ToolPodsList,
			"description": "List all Kubernetes Pods across all namespaces (or filtered by labelSelector/fieldSelector) via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"labelSelector": map[string]any{"type": "string"},
					"fieldSelector": map[string]any{"type": "string"},
				},
			},
		},
		{
			"name":        ToolPodsListInNamespace,
			"description": "List all Kubernetes Pods in a specific namespace via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"namespace":     map[string]any{"type": "string"},
					"labelSelector": map[string]any{"type": "string"},
					"fieldSelector": map[string]any{"type": "string"},
				},
				"required": []string{"namespace"},
			},
		},
		{
			"name":        ToolPodsGet,
			"description": "Get a Kubernetes Pod in the current or provided namespace via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":      map[string]any{"type": "string"},
					"namespace": map[string]any{"type": "string"},
				},
				"required": []string{"name"},
			},
		},
		{
			"name":        ToolPodsLog,
			"description": "Get logs of a Kubernetes Pod (including previous terminated container logs) via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":      map[string]any{"type": "string"},
					"namespace": map[string]any{"type": "string"},
					"container": map[string]any{"type": "string"},
					"previous":  map[string]any{"type": "boolean"},
					"tail":      map[string]any{"type": "integer"},
				},
				"required": []string{"name"},
			},
		},
		{
			"name":        ToolPodsTop,
			"description": "List CPU and memory consumption for Kubernetes Pods via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name":           map[string]any{"type": "string"},
					"namespace":      map[string]any{"type": "string"},
					"all_namespaces": map[string]any{"type": "boolean"},
					"label_selector": map[string]any{"type": "string"},
				},
			},
		},
		{
			"name":        ToolNodesStatsSummary,
			"description": "Get node resource usage and Kubernetes 1.35+ cgroup v2 PSI (Pressure Stall Information) metrics via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{"type": "string"},
				},
				"required": []string{"name"},
			},
		},
		{
			"name":        ToolResourcesList,
			"description": "List any Kubernetes or OpenShift resources by apiVersion and kind (e.g. Pod, StatefulSet, Deployment, Node, PersistentVolumeClaim, ResourceQuota) via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"apiVersion":    map[string]any{"type": "string"},
					"kind":          map[string]any{"type": "string"},
					"namespace":     map[string]any{"type": "string"},
					"labelSelector": map[string]any{"type": "string"},
					"fieldSelector": map[string]any{"type": "string"},
				},
				"required": []string{"apiVersion", "kind"},
			},
		},
		{
			"name":        ToolResourcesGet,
			"description": "Get any Kubernetes or OpenShift resource (StatefulSet, ResourceQuota, MachineSet, etc.) via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"apiVersion": map[string]any{"type": "string"},
					"kind":       map[string]any{"type": "string"},
					"name":       map[string]any{"type": "string"},
					"namespace":  map[string]any{"type": "string"},
				},
				"required": []string{"apiVersion", "kind", "name"},
			},
		},
		{
			"name":        ToolResourcesCreateOrUpdate,
			"description": "Create or update any Kubernetes/OpenShift resource via Server-Side Apply using containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"resource": map[string]any{"type": "string"},
				},
				"required": []string{"resource"},
			},
		},
		{
			"name":        ToolResourcesScale,
			"description": "Get or update the scale of a StatefulSet, Deployment, or OpenShift MachineSet via containers/kubernetes-mcp-server",
			"inputSchema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"apiVersion": map[string]any{"type": "string"},
					"kind":       map[string]any{"type": "string"},
					"name":       map[string]any{"type": "string"},
					"namespace":  map[string]any{"type": "string"},
					"scale":      map[string]any{"type": "integer"},
				},
				"required": []string{"apiVersion", "kind", "name"},
			},
		},
	}
}

