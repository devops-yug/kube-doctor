package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/luigi/kube-doctor/internal/api"
	"github.com/luigi/kube-doctor/internal/gitops"
	"github.com/luigi/kube-doctor/internal/hub"
	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/llm"
	"github.com/luigi/kube-doctor/internal/mcp"
	"github.com/luigi/kube-doctor/internal/operator"
	"github.com/luigi/kube-doctor/internal/rag"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/watcher"
)

func main() {
	if err := run(context.Background(), os.Args[1:], func(srv *http.Server) error {
		return srv.ListenAndServe()
	}); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func run(ctx context.Context, args []string, listenFn func(srv *http.Server) error) error {
	fs := flag.NewFlagSet("kube-doctor", flag.ContinueOnError)
	modeFlag := fs.String("mode", os.Getenv("KUBE_DOCTOR_MODE"), "Deployment mode: standalone | hub | agent")
	if err := fs.Parse(args); err != nil {
		return err
	}

	mode := *modeFlag
	if mode == "" {
		mode = "standalone"
	}

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	if addr[0] != ':' {
		addr = ":" + addr
	}

	knowledgePath := os.Getenv("KNOWLEDGE_BASE_PATH")
	if knowledgePath == "" {
		knowledgePath = "knowledge/k8s_1_35_ocp_4_22.json"
	}

	ragEngine := rag.NewEngine(knowledgePath)
	k8sClient := k8s.NewClient()
	k8sMCPClient := mcp.NewK8sMCPClient()
	gitopsManager := gitops.NewManager(k8sClient)
	remEngine := remediation.NewEngine(k8sClient, gitopsManager, k8sMCPClient)
	llmEngine := llm.NewEngine(ragEngine, remEngine, k8sClient)
	clusterWatcher := watcher.NewWatcher(k8sClient, remEngine, llmEngine)
	hubManager := hub.NewManager(mode, k8sClient, remEngine, clusterWatcher)
	operatorCtrl := operator.NewController(k8sClient, gitopsManager, remEngine)
	mcpServer := mcp.NewServer(ragEngine, remEngine, gitopsManager, k8sMCPClient)
	mcpServer.SetHubAndK8sClient(hubManager, k8sClient)
	apiServer := api.NewServer(llmEngine, remEngine, gitopsManager, k8sClient, mcpServer)
	apiServer.SetWatcher(clusterWatcher)
	apiServer.SetHubManager(hubManager)
	apiServer.SetOperatorController(operatorCtrl)

	clusterWatcher.Start(ctx)
	hubManager.Start(ctx)
	operatorCtrl.Start(ctx)

	srv := &http.Server{
		Addr:         addr,
		Handler:      apiServer,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 90 * time.Second,
	}

	log.Printf("kube-doctor (Kubernetes >= 1.35 / OpenShift >= 4.22 Specialized LLM & Remediation Gateway) listening on %s [mode=%s, cluster=%s]",
		addr, hubManager.Mode(), hubManager.LocalClusterID())
	log.Printf("  - OpenShift Console UI Plugin:     GET  http://localhost%s/ui", addr)
	log.Printf("  - OpenShift Operator Reconciler:   GET/POST http://localhost%s/v1/operator/status", addr)
	log.Printf("  - GitHub & GitLab OAuth GitOps:    GET/POST http://localhost%s/v1/gitops/status", addr)
	log.Printf("  - Autonomous Cluster Watcher:      active (live Kubernetes API + Operator CRD health loop)")
	log.Printf("  - Multi-Cluster Hub Fleet API:     GET/POST http://localhost%s/v1/hub/clusters", addr)
	log.Printf("  - Operator Adapters & Lineage API: GET  http://localhost%s/v1/adapters & /v1/hierarchy", addr)
	log.Printf("  - Alertmanager Webhook Receiver:   POST http://localhost%s/v1/webhooks/alertmanager", addr)
	log.Printf("  - Live Cluster Scan & Remediate:   POST http://localhost%s/v1/cluster/scan", addr)
	log.Printf("  - Autonomous Incidents Feed:       GET  http://localhost%s/v1/incidents", addr)
	log.Printf("  - OpenAI-compatible Chat API:      POST http://localhost%s/v1/chat/completions", addr)
	log.Printf("  - GitHub Copilot Extension:        POST http://localhost%s/copilot/agent", addr)
	log.Printf("  - MCP Server (+k8s-mcp-server):    POST http://localhost%s/mcp", addr)

	if err := listenFn(srv); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
