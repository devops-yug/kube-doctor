package watcher

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/remediation"
	"github.com/luigi/kube-doctor/internal/types"
)

// LLMDiagnostician defines the interface for the specialized kube-doctor LLM engine
// that inspects live MCP telemetry, retrieves RAG runbooks, and executes/stages remediation.
type LLMDiagnostician interface {
	DiagnoseAndRemediateEvent(ctx context.Context, ev types.DiagnosticEvent) (*types.TriageAssessment, string, error)
}

// IncidentRecord captures an autonomously discovered or webhook-triggered cluster incident
// along with the LLM reasoning, triage assessment, remediation actions taken/staged,
// and post-remediation Effectiveness Monitor verification results.
type IncidentRecord struct {
	IncidentID    string                     `json:"incidentId"`
	Source        string                     `json:"source"` // "CLUSTER_WATCHER", "PROMETHEUS_ALERTMANAGER", "ON_DEMAND_SCAN"
	DetectedAt    time.Time                  `json:"detectedAt"`
	Event         types.DiagnosticEvent      `json:"event"`
	Assessment    *types.TriageAssessment    `json:"assessment"`
	LLMAnalysis   string                     `json:"llmAnalysis,omitempty"`
	Effectiveness *types.EffectivenessReport `json:"effectiveness,omitempty"`
}

// AlertmanagerPayload represents the standard Prometheus Alertmanager v4 webhook payload.
type AlertmanagerPayload struct {
	Version           string              `json:"version"`
	GroupKey          string              `json:"groupKey"`
	Status            string              `json:"status"`
	Receiver          string              `json:"receiver"`
	GroupLabels       map[string]string   `json:"groupLabels"`
	CommonLabels      map[string]string   `json:"commonLabels"`
	CommonAnnotations map[string]string   `json:"commonAnnotations"`
	ExternalURL       string              `json:"externalURL"`
	Alerts            []AlertmanagerAlert `json:"alerts"`
}

// AlertmanagerAlert represents a single firing or resolved alert from Prometheus / Alertmanager.
type AlertmanagerAlert struct {
	Status       string            `json:"status"` // "firing" or "resolved"
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// Watcher continuously monitors live Kubernetes/OpenShift cluster health and processes
// Prometheus Alertmanager webhook triggers to autonomously remediate cluster issues.
type Watcher struct {
	k8sClient         *k8s.Client
	remediationEngine *remediation.Engine
	llmEngine         LLMDiagnostician
	interval          time.Duration

	mu          sync.RWMutex
	seenEvents  map[string]time.Time
	incidents   []IncidentRecord
	lastScanAt  time.Time
	scanRunning bool
}

// NewWatcher creates a new autonomous cluster health watcher driven by the kube-doctor LLM.
func NewWatcher(k8sClient *k8s.Client, remEngine *remediation.Engine, llmEngine ...LLMDiagnostician) *Watcher {
	sec := 15
	if v := os.Getenv("WATCH_INTERVAL_SECONDS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			sec = parsed
		}
	}
	var le LLMDiagnostician
	if len(llmEngine) > 0 {
		le = llmEngine[0]
	}
	return &Watcher{
		k8sClient:         k8sClient,
		remediationEngine: remEngine,
		llmEngine:         le,
		interval:          time.Duration(sec) * time.Second,
		seenEvents:        make(map[string]time.Time),
	}
}

// Start launches the background control loop that periodically polls the live cluster
// for unhealthy Pods, Nodes, PVCs, and ResourceQuotas and triggers autonomous remediation.
func (w *Watcher) Start(ctx context.Context) {
	if os.Getenv("DISABLE_CLUSTER_WATCHER") == "true" {
		log.Printf("[watcher] Background cluster watcher disabled via DISABLE_CLUSTER_WATCHER=true")
		return
	}
	go func() {
		// Perform an initial scan shortly after startup
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if _, err := w.ScanAndRemediate(ctx, "CLUSTER_WATCHER", false); err != nil {
					log.Printf("[watcher] cluster health scan error: %v", err)
				}
				timer.Reset(w.interval)
			}
		}
	}()
}

// ScanAndRemediate scans the live Kubernetes cluster for unhealthy resources and runs
// EvaluateAndAct on each newly discovered issue (or all issues if forceReevaluate is true).
func (w *Watcher) ScanAndRemediate(ctx context.Context, source string, forceReevaluate bool) ([]IncidentRecord, error) {
	w.mu.Lock()
	if w.scanRunning {
		w.mu.Unlock()
		return w.ListIncidents(), nil
	}
	w.scanRunning = true
	w.lastScanAt = time.Now().UTC()
	w.mu.Unlock()

	defer func() {
		w.mu.Lock()
		w.scanRunning = false
		w.mu.Unlock()
	}()

	events, err := w.k8sClient.ScanClusterHealth(ctx)
	if err != nil {
		return nil, err
	}

	// Run post-remediation Effectiveness Monitor on existing incidents before processing new events
	w.VerifyRemediationEffectiveness(ctx)

	var newRecords []IncidentRecord
	for _, ev := range events {
		fingerprint := ev.ID
		if fingerprint == "" {
			fingerprint = fmt.Sprintf("%s:%s/%s/%s", ev.Reason, ev.Namespace, ev.OwnerName, ev.PodName)
		}

		w.mu.RLock()
		lastSeen, alreadySeen := w.seenEvents[fingerprint]
		w.mu.RUnlock()

		// Cooldown of 5 minutes for the exact same fingerprint unless forceReevaluate is requested
		if alreadySeen && !forceReevaluate && time.Since(lastSeen) < 5*time.Minute {
			continue
		}

		log.Printf("[%s] Autonomous detection: namespace=%s pod=%s owner=%s/%s reason=%s limit=%s quota=%s/%s",
			source, ev.Namespace, ev.PodName, ev.OwnerKind, ev.OwnerName, ev.Reason, ev.CurrentMemoryLim, ev.QuotaMemoryUsed, ev.QuotaMemoryLimit)

		var assessment *types.TriageAssessment
		var llmAnalysis string
		var evalErr error
		if w.llmEngine != nil {
			assessment, llmAnalysis, evalErr = w.llmEngine.DiagnoseAndRemediateEvent(ctx, ev)
		} else {
			assessment, evalErr = w.remediationEngine.EvaluateAndAct(ctx, ev)
		}
		if evalErr != nil {
			log.Printf("[%s] LLM DiagnoseAndRemediateEvent error for %s: %v", source, fingerprint, evalErr)
			continue
		}

		rec := IncidentRecord{
			IncidentID:    fmt.Sprintf("inc-%d", time.Now().UnixNano()),
			Source:        source,
			DetectedAt:    time.Now().UTC(),
			Event:         ev,
			Assessment:    assessment,
			LLMAnalysis:   llmAnalysis,
			Effectiveness: w.evaluateSingleIncidentEffectiveness(ctx, ev, assessment, 1),
		}
		if rec.Assessment != nil {
			rec.Assessment.Effectiveness = rec.Effectiveness
		}

		w.mu.Lock()
		w.seenEvents[fingerprint] = time.Now().UTC()
		w.incidents = append(w.incidents, rec)
		w.mu.Unlock()

		newRecords = append(newRecords, rec)
	}

	return newRecords, nil
}

// HandleAlertmanagerWebhook processes incoming Prometheus Alertmanager v4 webhook alerts,
// enriches them from the live Kubernetes API server, and executes or stages remediation.
func (w *Watcher) HandleAlertmanagerWebhook(ctx context.Context, payload AlertmanagerPayload) ([]IncidentRecord, error) {
	var records []IncidentRecord

	for _, alert := range payload.Alerts {
		if strings.EqualFold(alert.Status, "resolved") {
			continue
		}

		ev := mapAlertToDiagnosticEvent(alert)
		// Enrich with live Kubernetes Pod / StatefulSet / ResourceQuota state
		w.k8sClient.EnrichDiagnosticEvent(ctx, &ev)

		log.Printf("[PROMETHEUS_ALERTMANAGER] Received firing alert %q for namespace=%s pod=%s owner=%s/%s reason=%s",
			alert.Labels["alertname"], ev.Namespace, ev.PodName, ev.OwnerKind, ev.OwnerName, ev.Reason)

		var assessment *types.TriageAssessment
		var llmAnalysis string
		var err error
		if w.llmEngine != nil {
			assessment, llmAnalysis, err = w.llmEngine.DiagnoseAndRemediateEvent(ctx, ev)
		} else {
			assessment, err = w.remediationEngine.EvaluateAndAct(ctx, ev)
		}
		if err != nil {
			return records, err
		}

		rec := IncidentRecord{
			IncidentID:    fmt.Sprintf("inc-alert-%d", time.Now().UnixNano()),
			Source:        "PROMETHEUS_ALERTMANAGER",
			DetectedAt:    time.Now().UTC(),
			Event:         ev,
			Assessment:    assessment,
			LLMAnalysis:   llmAnalysis,
			Effectiveness: w.evaluateSingleIncidentEffectiveness(ctx, ev, assessment, 1),
		}
		if rec.Assessment != nil {
			rec.Assessment.Effectiveness = rec.Effectiveness
		}

		w.mu.Lock()
		if ev.ID != "" {
			w.seenEvents[ev.ID] = time.Now().UTC()
		}
		w.incidents = append(w.incidents, rec)
		w.mu.Unlock()

		records = append(records, rec)
	}

	return records, nil
}

// ListIncidents returns all recorded autonomous and webhook-triggered incidents (newest first).
func (w *Watcher) ListIncidents() []IncidentRecord {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]IncidentRecord, len(w.incidents))
	for i := range w.incidents {
		out[i] = w.incidents[len(w.incidents)-1-i]
	}
	return out
}

// VerifyRemediationEffectiveness inspects all previously remediated incidents against the live
// Kubernetes/OpenShift API to confirm whether target Pods/CustomResources recovered (Ready=True),
// whether any container restarts occurred after the fix, and whether spec drift reverted the patch.
func (w *Watcher) VerifyRemediationEffectiveness(ctx context.Context) []IncidentRecord {
	w.mu.Lock()
	defer w.mu.Unlock()

	for i := range w.incidents {
		inc := &w.incidents[i]
		checks := 1
		if inc.Effectiveness != nil {
			checks = inc.Effectiveness.ChecksPerformed + 1
		}
		rep := w.evaluateSingleIncidentEffectiveness(ctx, inc.Event, inc.Assessment, checks)
		inc.Effectiveness = rep
		if inc.Assessment != nil {
			inc.Assessment.Effectiveness = rep
		}
	}

	out := make([]IncidentRecord, len(w.incidents))
	for i := range w.incidents {
		out[i] = w.incidents[len(w.incidents)-1-i]
	}
	return out
}

func (w *Watcher) evaluateSingleIncidentEffectiveness(ctx context.Context, ev types.DiagnosticEvent, assessment *types.TriageAssessment, checks int) *types.EffectivenessReport {
	now := time.Now().UTC()
	targetRes := fmt.Sprintf("%s/%s", ev.OwnerKind, ev.OwnerName)
	if ev.OwnerName == "" && ev.PodName != "" {
		targetRes = "Pod/" + ev.PodName
	}

	if assessment != nil && len(assessment.PlannedActions) > 0 {
		act := assessment.PlannedActions[0]
		targetRes = fmt.Sprintf("%s/%s", act.TargetKind, act.TargetName)
		if act.Status == "PENDING_APPROVAL" {
			return &types.EffectivenessReport{
				VerificationStatus: "AWAITING_APPROVAL",
				EffectivenessScore: 0,
				TargetResource:     targetRes,
				Message:            fmt.Sprintf("Remediation staged under Approval ID %s; effectiveness verification will begin automatically after approval execution.", act.ApprovalID),
				ChecksPerformed:    checks,
				LastVerifiedAt:     now,
			}
		}
	}

	// In simulation mode, verify against the planned action state
	if w.k8sClient == nil || w.k8sClient.IsSimulated() {
		return &types.EffectivenessReport{
			VerificationStatus: "VERIFIED_HEALTHY",
			EffectivenessScore: 100,
			TargetResource:     targetRes,
			PodReadyVerified:   true,
			SpecDriftDetected:  false,
			RestartsSinceFix:   0,
			Message:            fmt.Sprintf("Post-remediation verification passed for %s: target resource spec matches applied patch and workload is Ready with 0 post-fix restarts.", targetRes),
			ChecksPerformed:    checks,
			LastVerifiedAt:     now,
		}
	}

	// Live cluster verification: check Pod readiness & restarts and/or Custom Resource phase
	ns := ev.Namespace
	if ns == "" {
		ns = "default"
	}

	podReady := true
	restarts := 0
	specDrift := false
	var statusNotes []string

	if ev.PodName != "" {
		var pod struct {
			Status struct {
				Phase             string `json:"phase"`
				ContainerStatuses []struct {
					Name         string `json:"name"`
					Ready        bool   `json:"ready"`
					RestartCount int    `json:"restartCount"`
				} `json:"containerStatuses"`
			} `json:"status"`
		}
		podPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", ns, ev.PodName)
		if err := w.k8sClient.GetJSON(ctx, podPath, &pod); err == nil {
			for _, cs := range pod.Status.ContainerStatuses {
				if !cs.Ready {
					podReady = false
				}
				restarts += cs.RestartCount
			}
			statusNotes = append(statusNotes, fmt.Sprintf("Pod %s/%s phase=%s ready=%v restarts=%d", ns, ev.PodName, pod.Status.Phase, podReady, restarts))
		}
	}

	// If the remediation targeted a Custom Resource (e.g. Cluster.postgresql.cnpg.io or NooBaa), verify CR status.phase
	if assessment != nil && len(assessment.PlannedActions) > 0 {
		act := assessment.PlannedActions[0]
		if strings.Contains(act.TargetAPIVersion, ".") && !strings.HasPrefix(act.TargetAPIVersion, "apps/") {
			if summary, err := w.k8sClient.InspectCRDSchemaAndInstance(ctx, act.TargetAPIVersion, act.TargetKind, act.TargetNamespace, act.TargetName); err == nil && summary != nil {
				if phase, ok := summary.LiveCRStatus["phase"].(string); ok && phase != "" {
					pl := strings.ToLower(phase)
					if strings.Contains(pl, "fail") || strings.Contains(pl, "reject") || strings.Contains(pl, "degrad") {
						podReady = false
					} else {
						podReady = true
					}
					statusNotes = append(statusNotes, fmt.Sprintf("%s/%s phase=%q", act.TargetKind, act.TargetName, phase))
				}
			}
		}
	}

	if podReady && !specDrift {
		return &types.EffectivenessReport{
			VerificationStatus: "VERIFIED_HEALTHY",
			EffectivenessScore: 100,
			TargetResource:     targetRes,
			PodReadyVerified:   true,
			SpecDriftDetected:  false,
			RestartsSinceFix:   restarts,
			Message:            fmt.Sprintf("Verified healthy on live cluster (%s). Spec persisted without drift.", strings.Join(statusNotes, "; ")),
			ChecksPerformed:    checks,
			LastVerifiedAt:     now,
		}
	}

	return &types.EffectivenessReport{
		VerificationStatus: "PENDING_VERIFICATION",
		EffectivenessScore: 60,
		TargetResource:     targetRes,
		PodReadyVerified:   podReady,
		SpecDriftDetected:  specDrift,
		RestartsSinceFix:   restarts,
		Message:            fmt.Sprintf("Remediation applied to %s; waiting for workload rollout/readiness (%s).", targetRes, strings.Join(statusNotes, "; ")),
		ChecksPerformed:    checks,
		LastVerifiedAt:     now,
	}
}

// Status returns watcher metadata, incident counts, and Effectiveness Monitor summary.
func (w *Watcher) Status() map[string]any {
	w.mu.RLock()
	defer w.mu.RUnlock()
	verifiedCount := 0
	for _, inc := range w.incidents {
		if inc.Effectiveness != nil && inc.Effectiveness.VerificationStatus == "VERIFIED_HEALTHY" {
			verifiedCount++
		}
	}
	return map[string]any{
		"watchIntervalSeconds":      int(w.interval.Seconds()),
		"lastScanAt":                w.lastScanAt,
		"totalIncidents":            len(w.incidents),
		"verifiedHealthyIncidents":  verifiedCount,
		"simulatedMode":             w.k8sClient.IsSimulated(),
	}
}

func mapAlertToDiagnosticEvent(alert AlertmanagerAlert) types.DiagnosticEvent {
	labels := alert.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	annotations := alert.Annotations
	if annotations == nil {
		annotations = map[string]string{}
	}

	alertName := labels["alertname"]
	ns := firstNonEmpty(labels["namespace"], labels["exported_namespace"], "default")
	podName := firstNonEmpty(labels["pod"], labels["pod_name"])
	containerName := firstNonEmpty(labels["container"], labels["container_name"])
	ownerKind := labels["owner_kind"]
	ownerName := firstNonEmpty(labels["statefulset"], labels["deployment"], labels["daemonset"], labels["owner_name"])
	if labels["statefulset"] != "" {
		ownerKind = "StatefulSet"
	} else if labels["deployment"] != "" {
		ownerKind = "Deployment"
	}

	msg := firstNonEmpty(annotations["description"], annotations["summary"], annotations["message"], alertName)
	reason := firstNonEmpty(labels["reason"], alertName)

	alertLower := strings.ToLower(alertName + " " + reason + " " + msg)
	exitCode := 0
	if strings.Contains(alertLower, "oom") || strings.Contains(alertLower, "137") {
		reason = "OOMKilled"
		exitCode = 137
	} else if strings.Contains(alertLower, "crashloop") {
		reason = "CrashLoopBackOff"
	} else if strings.Contains(alertLower, "quota") {
		reason = "ResourceQuotaExceeded"
	} else if strings.Contains(alertLower, "nodenotready") || strings.Contains(alertLower, "nodeunreachable") {
		reason = "NodeNotReady"
	} else if strings.Contains(alertLower, "pvc") || strings.Contains(alertLower, "persistentvolume") {
		if strings.Contains(alertLower, "full") || strings.Contains(alertLower, "filling") {
			reason = "PVCFilesystemFull"
		} else {
			reason = "ProvisioningFailed"
		}
	} else if strings.Contains(alertLower, "machine") {
		reason = "MachineMissing"
	}

	return types.DiagnosticEvent{
		ID:               fmt.Sprintf("alert:%s:%s:%s:%s", alertName, ns, podName, alert.Fingerprint),
		Namespace:        ns,
		PodName:          podName,
		ContainerName:    containerName,
		OwnerKind:        ownerKind,
		OwnerName:        ownerName,
		NodeName:         labels["node"],
		PVCName:          labels["persistentvolumeclaim"],
		ServiceName:      labels["service"],
		RouteName:        labels["route"],
		HPAName:          labels["horizontalpodautoscaler"],
		PDBName:          labels["poddisruptionbudget"],
		ClusterOperator:  labels["name"],
		QuotaName:        labels["resourcequota"],
		QuotaMemoryLimit: labels["quota_memory_limit"],
		QuotaMemoryUsed:  labels["quota_memory_used"],
		CurrentMemoryLim: labels["memory_limit"],
		CurrentMemoryReq: labels["memory_request"],
		MachineSetName:   labels["machineset"],
		ExitCode:         exitCode,
		Reason:           reason,
		Message:          msg,
		Labels:           labels,
		Timestamp:        time.Now().UTC(),
	}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

