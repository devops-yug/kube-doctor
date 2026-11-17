package hub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
	"github.com/luigi/kube-doctor/internal/watcher"
)

// Mode defines the runtime deployment mode of kube-doctor.
//   - "standalone": All-in-one local cluster watcher + remediation + Hub fleet receiver + MCP server (default)
//   - "hub":        Central Multi-Cluster Fleet Control Plane + LLM/RAG + MCP Server + Command Dispatcher
//   - "agent":      Managed Spoke Cluster Agent pushing telemetry/incidents to Hub and executing Hub commands
type Mode string

const (
	ModeStandalone Mode = "standalone"
	ModeHub        Mode = "hub"
	ModeAgent      Mode = "agent"
)

// ManagedCluster represents a Kubernetes >= 1.35 or OpenShift >= 4.22 cluster
// registered with the kube-doctor Multi-Cluster Hub.
type ManagedCluster struct {
	ClusterID         string            `json:"clusterId"`
	DisplayName       string            `json:"displayName"`
	Role              string            `json:"role"` // "local-hub", "spoke-agent"
	Environment       string            `json:"environment,omitempty"`
	Region            string            `json:"region,omitempty"`
	KubernetesVersion string            `json:"kubernetesVersion"`
	OpenShiftVersion  string            `json:"openShiftVersion,omitempty"`
	AgentEndpoint     string            `json:"agentEndpoint,omitempty"`
	Status            string            `json:"status"` // "HEALTHY", "DEGRADED", "CRITICAL", "OFFLINE"
	NodeCount         int               `json:"nodeCount"`
	HealthyPodCount   int               `json:"healthyPodCount"`
	UnhealthyPodCount int               `json:"unhealthyPodCount"`
	ActiveIncidents   int               `json:"activeIncidents"`
	ResolvedIncidents int               `json:"resolvedIncidents,omitempty"`
	PendingApprovals  int               `json:"pendingApprovals"`
	InstalledAdapters []string          `json:"installedAdapters,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	RegisteredAt      time.Time         `json:"registeredAt"`
	LastHeartbeatAt   time.Time         `json:"lastHeartbeatAt"`
}

// AgentCommand represents a remediation action dispatched by the Central Hub
// to a specific managed Spoke Cluster Agent for execution.
type AgentCommand struct {
	CommandID   string                  `json:"commandId"`
	ClusterID   string                  `json:"clusterId"`
	IncidentID  string                  `json:"incidentId,omitempty"`
	Action      types.RemediationAction `json:"action"`
	Status      string                  `json:"status"` // "PENDING", "EXECUTED", "FAILED"
	ResultMsg   string                  `json:"resultMessage,omitempty"`
	CreatedAt   time.Time               `json:"createdAt"`
	CompletedAt *time.Time              `json:"completedAt,omitempty"`
}

// HeartbeatPayload is sent periodically by a Spoke Cluster Agent to the Hub.
type HeartbeatPayload struct {
	ClusterID         string                   `json:"clusterId"`
	DisplayName       string                   `json:"displayName,omitempty"`
	Environment       string                   `json:"environment,omitempty"`
	Region            string                   `json:"region,omitempty"`
	KubernetesVersion string                   `json:"kubernetesVersion,omitempty"`
	OpenShiftVersion  string                   `json:"openShiftVersion,omitempty"`
	AgentEndpoint     string                   `json:"agentEndpoint,omitempty"`
	NodeCount         int                      `json:"nodeCount"`
	HealthyPodCount   int                      `json:"healthyPodCount"`
	UnhealthyPodCount int                      `json:"unhealthyPodCount"`
	InstalledAdapters []string                 `json:"installedAdapters,omitempty"`
	Events            []types.DiagnosticEvent  `json:"events,omitempty"`
	Incidents         []watcher.IncidentRecord `json:"incidents,omitempty"`
}

// Manager coordinates multi-cluster registration, health aggregation, central incident triage,
// and remote remediation command dispatch across Hub and Spoke Agent deployments.
type Manager struct {
	mode              Mode
	localClusterID    string
	hubURL            string
	k8sClient         *k8s.Client
	remediationEngine *remediation.Engine
	localWatcher      *watcher.Watcher
	httpClient        *http.Client

	mu        sync.RWMutex
	clusters  map[string]*ManagedCluster
	incidents map[string][]watcher.IncidentRecord // clusterID -> incidents
	commands  map[string][]*AgentCommand          // clusterID -> command queue
}

// NewManager creates a Multi-Cluster Fleet Manager configured via KUBE_DOCTOR_MODE,
// KUBE_DOCTOR_CLUSTER_ID, and KUBE_DOCTOR_HUB_URL.
func NewManager(modeStr string, k8sClient *k8s.Client, remEngine *remediation.Engine, localWatcher *watcher.Watcher) *Manager {
	m := Mode(strings.ToLower(strings.TrimSpace(modeStr)))
	if m != ModeHub && m != ModeAgent {
		m = ModeStandalone
	}

	clusterID := strings.TrimSpace(os.Getenv("KUBE_DOCTOR_CLUSTER_ID"))
	if clusterID == "" {
		clusterID = "kind-local-cluster"
	}
	hubURL := strings.TrimRight(strings.TrimSpace(os.Getenv("KUBE_DOCTOR_HUB_URL")), "/")

	mgr := &Manager{
		mode:              m,
		localClusterID:    clusterID,
		hubURL:            hubURL,
		k8sClient:         k8sClient,
		remediationEngine: remEngine,
		localWatcher:      localWatcher,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
		},
		clusters:  make(map[string]*ManagedCluster),
		incidents: make(map[string][]watcher.IncidentRecord),
		commands:  make(map[string][]*AgentCommand),
	}

	// Register the local cluster in Hub/Standalone mode so fleet queries always show the local cluster
	now := time.Now().UTC()
	mgr.clusters[clusterID] = &ManagedCluster{
		ClusterID:         clusterID,
		DisplayName:       "Primary Local Cluster (" + clusterID + ")",
		Role:              string(m),
		Environment:       defaultEnv(os.Getenv("KUBE_DOCTOR_ENV"), "production"),
		Region:            defaultEnv(os.Getenv("KUBE_DOCTOR_REGION"), "local-kind"),
		KubernetesVersion: "v1.35.2",
		OpenShiftVersion:  "4.22.1",
		Status:            "HEALTHY",
		NodeCount:         1,
		InstalledAdapters: []string{"cloudnative-pg", "noobaa-odf", "strimzi-kafka", "percona-pg"},
		RegisteredAt:      now,
		LastHeartbeatAt:   now,
	}

	return mgr
}

// Mode returns the active deployment mode (standalone, hub, or agent).
func (m *Manager) Mode() Mode {
	return m.mode
}

// LocalClusterID returns the identifier of this cluster.
func (m *Manager) LocalClusterID() string {
	return m.localClusterID
}

	// Start launches the Spoke Agent -> Hub heartbeat & command sync loop when KUBE_DOCTOR_HUB_URL is configured.
func (m *Manager) Start(ctx context.Context) {
	if m.hubURL != "" {
		log.Printf("[spoke-agent:%s] Connecting to Central Hub at %s", m.localClusterID, m.hubURL)
	}
	// Periodically refresh local cluster stats and sync with Hub
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		m.refreshLocalClusterStats(ctx)
		if m.hubURL != "" {
			m.syncAgentWithHub(ctx)
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.refreshLocalClusterStats(ctx)
				if m.hubURL != "" {
					m.syncAgentWithHub(ctx)
				}
			}
		}
	}()
}

func (m *Manager) refreshLocalClusterStats(ctx context.Context) {
	var nodeCount, healthyPods, unhealthyPods int
	var k8sVer string
	if m.k8sClient != nil {
		nodeCount, healthyPods, unhealthyPods, k8sVer = m.k8sClient.GetClusterSummaryStats(ctx)
	}

	m.mu.Lock()
	c, ok := m.clusters[m.localClusterID]
	if !ok {
		m.mu.Unlock()
		return
	}
	c.LastHeartbeatAt = time.Now().UTC()
	if nodeCount > 0 {
		c.NodeCount = nodeCount
	}
	c.HealthyPodCount = healthyPods
	c.UnhealthyPodCount = unhealthyPods
	if k8sVer != "" {
		c.KubernetesVersion = k8sVer
	}
	if m.localWatcher != nil {
		incs := m.localWatcher.ListIncidents()
		c.ActiveIncidents = countActiveIncidents(incs)
		c.ResolvedIncidents = countResolvedIncidents(incs)
		m.incidents[m.localClusterID] = incs
	}
	if c.UnhealthyPodCount > 0 || c.ActiveIncidents > 0 {
		c.Status = "DEGRADED"
	} else {
		c.Status = "HEALTHY"
	}
	m.mu.Unlock()
}

func countActiveIncidents(incs []watcher.IncidentRecord) int {
	n := 0
	for _, inc := range incs {
		if !inc.IsResolved() {
			n++
		}
	}
	return n
}

func countResolvedIncidents(incs []watcher.IncidentRecord) int {
	n := 0
	for _, inc := range incs {
		if inc.IsResolved() {
			n++
		}
	}
	return n
}

func hasUnresolvedIncident(incs []watcher.IncidentRecord) bool {
	return countActiveIncidents(incs) > 0
}

// RegisterOrHeartbeat registers or updates a managed Spoke Cluster in the Hub.
func (m *Manager) RegisterOrHeartbeat(ctx context.Context, hb HeartbeatPayload) (*ManagedCluster, []watcher.IncidentRecord, error) {
	if hb.ClusterID == "" {
		return nil, nil, fmt.Errorf("clusterId is required")
	}
	now := time.Now().UTC()

	m.mu.Lock()
	cluster, exists := m.clusters[hb.ClusterID]
	if !exists {
		cluster = &ManagedCluster{
			ClusterID:    hb.ClusterID,
			DisplayName:  defaultEnv(hb.DisplayName, hb.ClusterID),
			Role:         "spoke-agent",
			RegisteredAt: now,
		}
		m.clusters[hb.ClusterID] = cluster
	}
	if hb.DisplayName != "" {
		cluster.DisplayName = hb.DisplayName
	}
	if hb.Environment != "" {
		cluster.Environment = hb.Environment
	}
	if hb.Region != "" {
		cluster.Region = hb.Region
	}
	if hb.KubernetesVersion != "" {
		cluster.KubernetesVersion = hb.KubernetesVersion
	} else if cluster.KubernetesVersion == "" {
		cluster.KubernetesVersion = "v1.35.2"
	}
	if hb.OpenShiftVersion != "" {
		cluster.OpenShiftVersion = hb.OpenShiftVersion
	}
	if hb.AgentEndpoint != "" {
		cluster.AgentEndpoint = hb.AgentEndpoint
	}
	if hb.NodeCount > 0 {
		cluster.NodeCount = hb.NodeCount
	}
	cluster.HealthyPodCount = hb.HealthyPodCount
	cluster.UnhealthyPodCount = hb.UnhealthyPodCount
	if len(hb.InstalledAdapters) > 0 {
		cluster.InstalledAdapters = hb.InstalledAdapters
	}
	cluster.LastHeartbeatAt = now
	m.mu.Unlock()

	// Process any incoming DiagnosticEvents from the Spoke Agent through the Hub's central Remediation/Adapter Engine
	var newIncidents []watcher.IncidentRecord
	for _, ev := range hb.Events {
		ev.ClusterID = hb.ClusterID
		m.mu.RLock()
		alreadyRecorded := false
		for _, existingInc := range m.incidents[hb.ClusterID] {
			if (ev.ID != "" && existingInc.Event.ID == ev.ID) ||
				(ev.PodName != "" && existingInc.Event.Namespace == ev.Namespace && existingInc.Event.PodName == ev.PodName && existingInc.Event.Reason == ev.Reason) {
				alreadyRecorded = true
				break
			}
		}
		m.mu.RUnlock()
		if alreadyRecorded {
			continue
		}

		assessment, err := m.remediationEngine.EvaluateAndAct(ctx, ev)
		if err != nil {
			continue
		}
		rec := watcher.IncidentRecord{
			IncidentID: fmt.Sprintf("inc-%s-%d", hb.ClusterID, time.Now().UnixNano()),
			ClusterID:  hb.ClusterID,
			Status:     "ACTIVE",
			Source:     "SPOKE_AGENT:" + hb.ClusterID,
			DetectedAt: now,
			Event:      ev,
			Assessment: assessment,
		}
		newIncidents = append(newIncidents, rec)

		m.mu.Lock()
		m.incidents[hb.ClusterID] = append(m.incidents[hb.ClusterID], rec)
		// For any AUTO_REMEDIATE action on a remote spoke cluster, enqueue an AgentCommand for the spoke to execute locally
		for _, act := range assessment.PlannedActions {
			if act.Policy == types.PolicyAutoRemediate {
				cmd := &AgentCommand{
					CommandID:  fmt.Sprintf("cmd-%s-%d", hb.ClusterID, time.Now().UnixNano()),
					ClusterID:  hb.ClusterID,
					IncidentID: rec.IncidentID,
					Action:     act,
					Status:     "PENDING",
					CreatedAt:  now,
				}
				m.commands[hb.ClusterID] = append(m.commands[hb.ClusterID], cmd)
			}
		}
		m.mu.Unlock()
	}

	if len(hb.Incidents) > 0 {
		m.mu.Lock()
		existing := m.incidents[hb.ClusterID]
		idxByID := make(map[string]int, len(existing))
		for i, inc := range existing {
			idxByID[inc.IncidentID] = i
		}
		for _, inc := range hb.Incidents {
			inc.ClusterID = hb.ClusterID
			inc.Event.ClusterID = hb.ClusterID
			if inc.Source == "" || inc.Source == "CLUSTER_WATCHER" {
				inc.Source = "SPOKE_AGENT:" + hb.ClusterID
			}
			if inc.Event.NodeName != "" && !strings.Contains(inc.Event.NodeName, hb.ClusterID) {
				inc.Event.NodeName = fmt.Sprintf("%s (%s)", inc.Event.NodeName, hb.ClusterID)
			}
			if inc.IsResolved() {
				inc.Status = "RESOLVED"
				if inc.ResolvedAt == nil {
					t := now
					inc.ResolvedAt = &t
				}
			} else {
				inc.Status = "ACTIVE"
			}
			if idx, ok := idxByID[inc.IncidentID]; ok {
				existing[idx] = inc
			} else {
				idxByID[inc.IncidentID] = len(existing)
				existing = append(existing, inc)
			}
		}
		m.incidents[hb.ClusterID] = existing
		m.mu.Unlock()
	}

	m.mu.Lock()
	cluster.ActiveIncidents = countActiveIncidents(m.incidents[hb.ClusterID])
	cluster.ResolvedIncidents = countResolvedIncidents(m.incidents[hb.ClusterID])
	if cluster.UnhealthyPodCount > 0 || cluster.ActiveIncidents > 0 {
		cluster.Status = "DEGRADED"
	} else {
		cluster.Status = "HEALTHY"
	}
	clusterCopy := *cluster
	m.mu.Unlock()

	if clusterCopy.Status == "DEGRADED" || len(newIncidents) > 0 {
		log.Printf("[hub] Spoke cluster %s (%s) status=%s nodeCount=%d healthyPods=%d unhealthyPods=%d activeIncidents=%d",
			clusterCopy.ClusterID, clusterCopy.DisplayName, clusterCopy.Status,
			clusterCopy.NodeCount, clusterCopy.HealthyPodCount, clusterCopy.UnhealthyPodCount, clusterCopy.ActiveIncidents)
	}

	return &clusterCopy, newIncidents, nil
}

// DispatchCommand enqueues a remediation action for execution on a target ManagedCluster.
// If the target cluster is the local cluster, it executes immediately via the local RemediationEngine.
func (m *Manager) DispatchCommand(ctx context.Context, clusterID string, action types.RemediationAction) (*AgentCommand, error) {
	if clusterID == "" {
		clusterID = m.localClusterID
	}
	action.ClusterID = clusterID
	now := time.Now().UTC()

	cmd := &AgentCommand{
		CommandID: fmt.Sprintf("cmd-%s-%d", clusterID, now.UnixNano()),
		ClusterID: clusterID,
		Action:    action,
		Status:    "PENDING",
		CreatedAt: now,
	}

	// If targeting the local cluster, execute immediately
	if clusterID == m.localClusterID && m.remediationEngine != nil {
		if err := m.remediationEngine.ExecuteApprovedAction(ctx, action); err != nil {
			cmd.Status = "FAILED"
			cmd.ResultMsg = err.Error()
		} else {
			cmd.Status = "EXECUTED"
			cmd.ResultMsg = "Executed directly on local cluster " + clusterID
		}
		cmd.CompletedAt = &now
	}

	m.mu.Lock()
	m.commands[clusterID] = append(m.commands[clusterID], cmd)
	m.mu.Unlock()

	return cmd, nil
}

// PendingCommands returns all PENDING commands queued for a spoke cluster agent.
func (m *Manager) PendingCommands(clusterID string) []*AgentCommand {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*AgentCommand
	for _, cmd := range m.commands[clusterID] {
		if cmd.Status == "PENDING" {
			out = append(out, cmd)
		}
	}
	return out
}

// AckCommand marks a dispatched command as EXECUTED or FAILED by the spoke cluster agent.
func (m *Manager) AckCommand(clusterID, commandID, status, resultMsg string) (*AgentCommand, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cmd := range m.commands[clusterID] {
		if cmd.CommandID == commandID {
			if status == "" {
				status = "EXECUTED"
			}
			cmd.Status = status
			cmd.ResultMsg = resultMsg
			now := time.Now().UTC()
			cmd.CompletedAt = &now
			return cmd, nil
		}
	}
	return nil, fmt.Errorf("command %q not found for cluster %q", commandID, clusterID)
}

// ListClusters returns all registered clusters in the fleet.
func (m *Manager) ListClusters() []ManagedCluster {
	m.refreshLocalClusterStats(context.Background())
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ManagedCluster, 0, len(m.clusters))
	for _, c := range m.clusters {
		out = append(out, *c)
	}
	return out
}

// ListFleetIncidents returns incidents across all clusters or filtered by clusterID.
func (m *Manager) ListFleetIncidents(clusterID string) []watcher.IncidentRecord {
	m.refreshLocalClusterStats(context.Background())
	m.mu.RLock()
	defer m.mu.RUnlock()
	if clusterID != "" {
		return m.incidents[clusterID]
	}
	var all []watcher.IncidentRecord
	for _, list := range m.incidents {
		all = append(all, list...)
	}
	return all
}

// ListActiveFleetIncidents returns only unresolved (ACTIVE) incidents across all clusters or filtered by clusterID.
func (m *Manager) ListActiveFleetIncidents(clusterID string) []watcher.IncidentRecord {
	all := m.ListFleetIncidents(clusterID)
	out := make([]watcher.IncidentRecord, 0)
	for _, inc := range all {
		if !inc.IsResolved() {
			out = append(out, inc)
		}
	}
	return out
}

// ListResolvedFleetIncidents returns resolved/verified-healthy historical incidents across all clusters or filtered by clusterID.
func (m *Manager) ListResolvedFleetIncidents(clusterID string) []watcher.IncidentRecord {
	all := m.ListFleetIncidents(clusterID)
	out := make([]watcher.IncidentRecord, 0)
	for _, inc := range all {
		if inc.IsResolved() {
			out = append(out, inc)
		}
	}
	return out
}

// syncAgentWithHub pushes local heartbeat & events to the remote Hub and executes any pending Hub commands.
func (m *Manager) syncAgentWithHub(ctx context.Context) {
	hb := HeartbeatPayload{
		ClusterID:         m.localClusterID,
		DisplayName:       defaultEnv(os.Getenv("KUBE_DOCTOR_DISPLAY_NAME"), "Spoke Cluster ("+m.localClusterID+")"),
		Environment:       defaultEnv(os.Getenv("KUBE_DOCTOR_ENV"), "edge"),
		Region:            defaultEnv(os.Getenv("KUBE_DOCTOR_REGION"), "raspberry-pi-lan"),
		KubernetesVersion: "v1.35.2",
		OpenShiftVersion:  "4.22.1",
		InstalledAdapters: []string{"cloudnative-pg", "noobaa-odf", "strimzi-kafka", "percona-pg"},
	}
	if m.k8sClient != nil {
		nodeCount, healthyPods, unhealthyPods, k8sVer := m.k8sClient.GetClusterSummaryStats(ctx)
		hb.NodeCount = nodeCount
		hb.HealthyPodCount = healthyPods
		hb.UnhealthyPodCount = unhealthyPods
		if k8sVer != "" {
			hb.KubernetesVersion = k8sVer
		}
	}
	if m.localWatcher != nil {
		hb.Incidents = m.localWatcher.ListIncidents()
		if hb.UnhealthyPodCount > 0 && len(hb.Incidents) == 0 {
			if _, err := m.localWatcher.ScanAndRemediate(ctx, "SPOKE_AGENT:"+m.localClusterID, false); err == nil {
				hb.Incidents = m.localWatcher.ListIncidents()
			}
		}
	}
	if m.k8sClient != nil && hb.UnhealthyPodCount > 0 && len(hb.Incidents) == 0 {
		if liveEvents, err := m.k8sClient.ScanClusterHealth(ctx); err == nil && len(liveEvents) > 0 {
			hb.Events = liveEvents
		}
	}
	bodyBytes, _ := json.Marshal(hb)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.hubURL+"/v1/hub/clusters/heartbeat", bytes.NewReader(bodyBytes))
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		resp, doErr := m.httpClient.Do(req)
		if doErr != nil {
			log.Printf("[spoke-agent:%s] Heartbeat to Hub %s failed: %v", m.localClusterID, m.hubURL, doErr)
			return
		}
		_, _ = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}

	// Poll pending commands from Hub
	cmdURL := fmt.Sprintf("%s/v1/hub/clusters/%s/commands", m.hubURL, m.localClusterID)
	cmdReq, err := http.NewRequestWithContext(ctx, http.MethodGet, cmdURL, nil)
	if err != nil {
		return
	}
	resp, err := m.httpClient.Do(cmdReq)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var parsed struct {
		Commands []AgentCommand `json:"commands"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return
	}
	for _, cmd := range parsed.Commands {
		status := "EXECUTED"
		msg := "Executed on spoke agent " + m.localClusterID
		if m.remediationEngine != nil {
			if execErr := m.remediationEngine.ExecuteApprovedAction(ctx, cmd.Action); execErr != nil {
				status = "FAILED"
				msg = execErr.Error()
			}
		}
		log.Printf("[spoke-agent:%s] Executed Hub command %s (%s): %s", m.localClusterID, cmd.CommandID, cmd.Action.Operation, status)
		ackPayload, _ := json.Marshal(map[string]string{"status": status, "resultMessage": msg})
		ackURL := fmt.Sprintf("%s/v1/hub/clusters/%s/commands/%s/ack", m.hubURL, m.localClusterID, cmd.CommandID)
		if ackReq, aErr := http.NewRequestWithContext(ctx, http.MethodPost, ackURL, bytes.NewReader(ackPayload)); aErr == nil {
			ackReq.Header.Set("Content-Type", "application/json")
			if ackResp, dErr := m.httpClient.Do(ackReq); dErr == nil {
				_ = ackResp.Body.Close()
			}
		}
	}
}

func defaultEnv(val, fallback string) string {
	if strings.TrimSpace(val) != "" {
		return strings.TrimSpace(val)
	}
	return fallback
}
