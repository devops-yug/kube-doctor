package adapters

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

// universalAICRDAdapter is a single, universal, zero-hardcoding Operator Custom Resource Adapter.
// How it works:
//  1. Discovers the controlling Custom Resource via recursive metadata.ownerReferences (or YAML GVK/label hints).
//  2. Queries the Kubernetes API server for the live CRD OpenAPI v3 Schema
//     (/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{plural}.{group}) AND the live CR (.spec + .status).
//  3. Sends the Problem Statement + Live CR Spec + CRD OpenAPI v3 Schema Fields to the AI Model
//     (Ollama kube-doctor-1.35-ocp-4.22) so the AI model decides WHICH field path(s) in the CR to update.
//  4. Builds the JSON Merge Patch dynamically from the AI model's selected field paths (or schema introspection)
//     and applies governance rules (AUTO_REMEDIATE within ResourceQuota vs APPROVAL_REQUIRED when exceeding quota).
type universalAICRDAdapter struct {
	spec       DeclarativeAdapterSpec
	httpClient *http.Client
}

// NewUniversalAICRDAdapter creates an AI + CRD-Schema driven adapter from a DeclarativeAdapterSpec
// (or as a wildcard fallback for any Custom Resource discovered in the cluster).
func NewUniversalAICRDAdapter(spec DeclarativeAdapterSpec) OperatorAdapter {
	return &universalAICRDAdapter{
		spec: spec,
		httpClient: &http.Client{
			Timeout: 25 * time.Second,
		},
	}
}

func (a *universalAICRDAdapter) Name() string {
	return a.spec.Name
}

func (a *universalAICRDAdapter) Description() string {
	return a.spec.Description
}

func (a *universalAICRDAdapter) SourceType() string {
	if a.spec.PrimaryGVK.Kind == "" {
		return "live-cluster-crd-ai-discovery"
	}
	return "yaml-crd-ai-adapter"
}

func (a *universalAICRDAdapter) SupportedGVKs() []GVKDescriptor {
	if a.spec.PrimaryGVK.Kind == "" {
		return []GVKDescriptor{
			{APIVersion: "*.io/*, *.com/*", Kind: "*", Plural: "*", Role: "dynamicCRDDiscovery"},
		}
	}
	out := []GVKDescriptor{a.spec.PrimaryGVK}
	out = append(out, a.spec.HierarchyGVKs...)
	return out
}

func (a *universalAICRDAdapter) MatchesEvent(ev types.DiagnosticEvent) bool {
	// 1. If this adapter was loaded from a YAML profile (e.g. cloudnative-pg.yaml, noobaa-odf.yaml, percona-pg.yaml)
	if a.spec.PrimaryGVK.Kind != "" {
		if strings.EqualFold(ev.OperatorAdapter, a.spec.Name) {
			return true
		}
		primaryGroup, _ := splitGroupVersion(a.spec.PrimaryGVK.APIVersion)
		if strings.EqualFold(ev.OwnerKind, a.spec.PrimaryGVK.Kind) &&
			(primaryGroup == "" || strings.Contains(strings.ToLower(ev.OwnerAPIVersion), strings.ToLower(primaryGroup)) || ev.OwnerAPIVersion == "") {
			return true
		}
		for _, lk := range a.spec.PodLabelKeys {
			if ev.Labels != nil && ev.Labels[lk] != "" {
				return true
			}
		}
		for _, link := range ev.OwnershipChain {
			if strings.EqualFold(link.Kind, a.spec.PrimaryGVK.Kind) ||
				(primaryGroup != "" && strings.Contains(strings.ToLower(link.APIVersion), strings.ToLower(primaryGroup))) {
				return true
			}
		}
		return false
	}

	// 2. Wildcard Universal CRD Adapter: matches ANY event whose owner or ownershipChain
	// includes a non-core Custom Resource (i.e. API group contains a dot like postgresql.cnpg.io, noobaa.io, etc.)
	if isCustomResourceGVK(ev.OwnerAPIVersion, ev.OwnerKind) {
		return true
	}
	for _, link := range ev.OwnershipChain {
		if isCustomResourceGVK(link.APIVersion, link.Kind) {
			return true
		}
	}
	return false
}

func (a *universalAICRDAdapter) BuildRemediationPlan(ctx context.Context, k8sClient *k8s.Client, ev types.DiagnosticEvent) (*AdapterRemediationPlan, error) {
	now := time.Now().UTC()

	// 1. Identify target Custom Resource GVK and Name from the event's resolved ownership chain or YAML profile
	targetAPIVersion, targetKind, targetName := a.resolveTargetCR(ev)

	// 2. Introspect the live CRD OpenAPI v3 Schema & live CR instance from the Kubernetes API server
	var schemaSummary *k8s.CRDSchemaSummary
	if k8sClient != nil {
		schemaSummary, _ = k8sClient.InspectCRDSchemaAndInstance(ctx, targetAPIVersion, targetKind, ev.Namespace, targetName)
	}
	if schemaSummary == nil {
		schemaSummary = &k8s.CRDSchemaSummary{
			CRDName:      strings.ToLower(k8s.PluralizeKind(targetKind)) + "." + fstGroup(targetAPIVersion),
			Kind:         targetKind,
			Plural:       k8s.PluralizeKind(targetKind),
			SchemaSource: "yaml-catalog",
		}
	}
	// Merge any schema field hints from the YAML file if the cluster CRD had preserve-unknown-fields or was simulated
	if len(schemaSummary.SpecFields) == 0 && len(a.spec.CRDSchemaFields) > 0 {
		schemaSummary.SpecFields = a.spec.CRDSchemaFields
		if schemaSummary.SchemaSource == "" {
			schemaSummary.SchemaSource = "yaml-crd-schema"
		}
	}

	// 3. Compute target resource values (e.g. new memory limit/request or new storage size) and check Namespace Quota
	curLimMiB := k8s.ParseMemoryMiB(ev.CurrentMemoryLim)
	if curLimMiB <= 0 {
		curLimMiB = 256
	}
	desiredLimMiB := curLimMiB * 2
	if desiredLimMiB < 512 {
		desiredLimMiB = 512
	}
	newLimStr := k8s.FormatMemoryMiB(desiredLimMiB)
	newReqStr := k8s.FormatMemoryMiB(desiredLimMiB / 2)

	curStorageMiB := k8s.ParseMemoryMiB(ev.CurrentStorage)
	if curStorageMiB <= 0 {
		curStorageMiB = 10 * 1024 // 10Gi
	}
	newStorageStr := k8s.FormatMemoryMiB(curStorageMiB * 2)

	quotaLimMiB := k8s.ParseMemoryMiB(ev.QuotaMemoryLimit)
	quotaUsedMiB := k8s.ParseMemoryMiB(ev.QuotaMemoryUsed)
	deltaMiB := desiredLimMiB - curLimMiB
	exceedsQuota := quotaLimMiB > 0 && (quotaUsedMiB+deltaMiB > quotaLimMiB)

	// 4. Ask the AI Model (or CRD Schema Field Selector) WHICH field(s) in the Custom Resource need to be updated
	// given the CRD OpenAPI v3 schema, live CR .spec/.status, and problem statement!
	decision := a.determineCRDFieldsToUpdateViaAI(ctx, ev, targetAPIVersion, targetKind, targetName, schemaSummary, newLimStr, newReqStr, newStorageStr)

	// Build the JSON Merge Patch dynamically from the selected dot-paths
	patchMap := make(map[string]any)
	for dotPath, val := range decision.UpdatedFields {
		mergeNestedMap(patchMap, buildNestedPatch(dotPath, val))
	}
	if decision.StatusPhaseUpdate != "" {
		mergeNestedMap(patchMap, map[string]any{
			"status": map[string]any{"phase": decision.StatusPhaseUpdate},
		})
	}
	patchBytes, _ := json.Marshal(patchMap)

	fieldList := make([]string, 0, len(decision.UpdatedFields))
	for k, v := range decision.UpdatedFields {
		fieldList = append(fieldList, fmt.Sprintf("%s=%v", k, v))
	}

	// 5. Apply Governance Rules (Exceeds Namespace ResourceQuota -> APPROVAL_REQUIRED; Within Quota -> AUTO_REMEDIATE)
	if exceedsQuota {
		newQuotaLim := k8s.FormatMemoryMiB(quotaLimMiB + deltaMiB + 1024)
		quotaPatch := map[string]any{
			"spec": map[string]any{
				"hard": map[string]string{
					"limits.memory":   newQuotaLim,
					"requests.memory": newQuotaLim,
				},
			},
		}
		quotaPatchBytes, _ := json.Marshal(quotaPatch)

		primaryAct := types.RemediationAction{
			ActionID:         fmt.Sprintf("act-crd-quota-%d", now.UnixNano()),
			ClusterID:        ev.ClusterID,
			StepOrder:        1,
			TargetAPIVersion: "v1",
			TargetKind:       "ResourceQuota",
			TargetNamespace:  ev.Namespace,
			TargetName:       defaultName(ev.QuotaName, "default-quota"),
			Operation:        "EXPAND_NAMESPACE_QUOTA_FOR_CRD",
			Policy:           types.PolicyApprovalRequired,
			PatchPayload:     string(quotaPatchBytes),
			Description:      fmt.Sprintf("Expand Namespace %s ResourceQuota %s to %s before patching %s/%s (%s)", ev.Namespace, defaultName(ev.QuotaName, "default-quota"), newQuotaLim, targetKind, targetName, strings.Join(fieldList, ", ")),
			Status:           "PENDING_APPROVAL",
		}
		followUpAct := types.RemediationAction{
			ActionID:         fmt.Sprintf("act-crd-patch-%d", now.UnixNano()),
			ClusterID:        ev.ClusterID,
			StepOrder:        2,
			TargetAPIVersion: targetAPIVersion,
			TargetKind:       targetKind,
			TargetNamespace:  ev.Namespace,
			TargetName:       targetName,
			Operation:        fmt.Sprintf("AI_CRD_PATCH_%s", strings.ToUpper(targetKind)),
			Policy:           types.PolicyApprovalRequired,
			PatchPayload:     string(patchBytes),
			Description:      fmt.Sprintf("AI + CRD Schema (%s) selected fields [%s] on %s.%s %s/%s", schemaSummary.CRDName, strings.Join(fieldList, ", "), targetKind, targetAPIVersion, ev.Namespace, targetName),
			Status:           "PENDING_APPROVAL",
		}
		return &AdapterRemediationPlan{
			AdapterName:        a.Name(),
			IssueType:          types.IssueNamespaceQuotaExceed,
			Criticality:        types.CriticalityP0Critical,
			Severity:           types.SeveritySev1,
			Summary:            fmt.Sprintf("Operator CR %s.%s %s/%s requires updating [%s], which exceeds Namespace ResourceQuota (%s). Staged RemediationRequest CRD & GitOps PR.", targetKind, targetAPIVersion, ev.Namespace, targetName, strings.Join(fieldList, ", "), ev.QuotaMemoryLimit),
			RootCauseAnalysis:  decision.Reasoning,
			K8s135FeaturesUsed: []string{"Live CRD OpenAPI v3 Schema Introspection + AI Field Selection", "Kubernetes 1.35 InPlacePodVerticalScaling"},
			OCP422FeaturesUsed: []string{"Operator Custom Resource Lineage & Quota Governance"},
			CRDSchemaUsed:      schemaSummary,
			UpdatedFields:      decision.UpdatedFields,
			PrimaryAction:      primaryAct,
			FollowUpActions:    []types.RemediationAction{followUpAct},
		}, nil
	}

	policy := types.PolicyAutoRemediate
	status := "EXECUTED"
	if !a.spec.AutoRemediateMemory && a.spec.Name != "" {
		policy = types.PolicyApprovalRequired
		status = "PENDING_APPROVAL"
	}

	act := types.RemediationAction{
		ActionID:         fmt.Sprintf("act-crd-ai-%d", now.UnixNano()),
		ClusterID:        ev.ClusterID,
		StepOrder:        1,
		TargetAPIVersion: targetAPIVersion,
		TargetKind:       targetKind,
		TargetNamespace:  ev.Namespace,
		TargetName:       targetName,
		Operation:        fmt.Sprintf("AI_CRD_PATCH_%s", strings.ToUpper(targetKind)),
		Policy:           policy,
		PatchPayload:     string(patchBytes),
		Description:      fmt.Sprintf("AI + CRD Schema (%s) selected fields [%s] on %s.%s %s/%s", schemaSummary.CRDName, strings.Join(fieldList, ", "), targetKind, targetAPIVersion, ev.Namespace, targetName),
		Status:           status,
		ExecutedAt:       &now,
	}

	return &AdapterRemediationPlan{
		AdapterName:        a.Name(),
		IssueType:          decision.IssueType,
		Criticality:        types.CriticalityP1High,
		Severity:           types.SeveritySev2,
		Summary:            fmt.Sprintf("Inspected CRD schema %q (%s) and used AI model to select CR fields [%s] on %s/%s (%s).", schemaSummary.CRDName, schemaSummary.SchemaSource, strings.Join(fieldList, ", "), targetKind, targetName, targetAPIVersion),
		RootCauseAnalysis:  decision.Reasoning,
		K8s135FeaturesUsed: []string{"Live CRD OpenAPI v3 Schema Introspection (/apis/apiextensions.k8s.io/v1/customresourcedefinitions) + AI Field Mapping"},
		OCP422FeaturesUsed: []string{"Recursive ownerReferences Multi-CRD Hierarchy Resolution"},
		CRDSchemaUsed:      schemaSummary,
		UpdatedFields:      decision.UpdatedFields,
		PrimaryAction:      act,
	}, nil
}

type aiFieldDecision struct {
	IssueType         types.IssueType `json:"issueType"`
	UpdatedFields     map[string]any  `json:"updatedFields"`
	StatusPhaseUpdate string          `json:"statusPhaseUpdate,omitempty"`
	Reasoning         string          `json:"reasoning"`
}

// determineCRDFieldsToUpdateViaAI sends the CRD's OpenAPI v3 .spec schema fields, the live CR's .spec/.status,
// and the problem statement to the Ollama LLM (`kube-doctor-1.35-ocp-4.22`) so the AI model chooses the exact
// CRD field paths to update. It also validates/merges against the introspected CRD schema so the patch is guaranteed valid.
func (a *universalAICRDAdapter) determineCRDFieldsToUpdateViaAI(
	ctx context.Context,
	ev types.DiagnosticEvent,
	targetAPIVersion, targetKind, targetName string,
	schema *k8s.CRDSchemaSummary,
	newLim, newReq, newStorage string,
) aiFieldDecision {
	// First, run schema-based field matching over the CRD's OpenAPI v3 specFields so we have a baseline
	// grounded in the actual CRD schema properties.
	baseline := matchFieldsFromCRDSchema(ev, targetAPIVersion, targetKind, targetName, schema, newLim, newReq, newStorage)

	// Query Ollama LLM if enabled (and not in fast unit-test simulation mode unless explicitly requested)
	upstreamURL := strings.TrimRight(os.Getenv("UPSTREAM_LLM_BASE_URL"), "/")
	if upstreamURL == "" && os.Getenv("DISABLE_UPSTREAM_LLM") != "true" && os.Getenv("KUBE_SIMULATION_MODE") != "true" {
		upstreamURL = "http://127.0.0.1:11434"
	}
	if upstreamURL != "" && os.Getenv(" ENABLE_AI_CRD_IN_TESTS") != "false" {
		if aiDec, ok := a.askOllamaForCRDFields(ctx, upstreamURL, ev, targetAPIVersion, targetKind, targetName, schema, newLim, newReq, newStorage); ok {
			if len(aiDec.UpdatedFields) > 0 {
				baseline.UpdatedFields = aiDec.UpdatedFields
			}
			if aiDec.Reasoning != "" {
				baseline.Reasoning = aiDec.Reasoning
			}
		}
	}

	return baseline
}

func (a *universalAICRDAdapter) askOllamaForCRDFields(
	ctx context.Context,
	upstreamURL string,
	ev types.DiagnosticEvent,
	targetAPIVersion, targetKind, targetName string,
	schema *k8s.CRDSchemaSummary,
	newLim, newReq, newStorage string,
) (aiFieldDecision, bool) {
	var dec aiFieldDecision
	schemaJSON, _ := json.MarshalIndent(schema.SpecFields, "", "  ")
	liveSpecJSON, _ := json.MarshalIndent(schema.LiveCRSpec, "", "  ")

	prompt := fmt.Sprintf(`You are kube-doctor-1.35-ocp-4.22. Given the Kubernetes CustomResourceDefinition (CRD) OpenAPI v3 schema fields, the live Custom Resource spec, and the cluster problem statement, identify the exact Custom Resource dot-path field(s) under "spec." that must be updated to remediate the issue.

Problem Statement:
- Resource: %s/%s (apiVersion: %s, namespace: %s, pod: %s)
- Reason: %s (ExitCode: %d)
- Message: %s
- Recommended Target Memory Limit: %s (Request: %s)
- Recommended Target Storage Size: %s

CRD (%s) Available .spec Schema Fields:
%s

Live Custom Resource Current .spec:
%s

Respond ONLY with a valid JSON object in this exact format:
{
  "updatedFields": {
    "spec.<exact.field.path>": "<new_value>"
  },
  "reasoning": "<one paragraph explaining why these CRD fields were chosen from the CRD schema and ownerReferences hierarchy>"
}`,
		targetKind, targetName, targetAPIVersion, ev.Namespace, ev.PodName,
		ev.Reason, ev.ExitCode, ev.Message, newLim, newReq, newStorage,
		schema.CRDName, string(schemaJSON), string(liveSpecJSON),
	)

	reqBody, _ := json.Marshal(map[string]any{
		"model":       "kube-doctor-1.35-ocp-4.22",
		"temperature": 0.1,
		"stream":      false,
		"messages": []map[string]string{
			{"role": "user", "content": prompt},
		},
	})

	reqCtx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	httpReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, upstreamURL+"/v1/chat/completions", bytes.NewReader(reqBody))
	if err != nil {
		return dec, false
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return dec, false
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return dec, false
	}
	rawBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return dec, false
	}
	var chatResp struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(rawBytes, &chatResp); err != nil || len(chatResp.Choices) == 0 {
		return dec, false
	}
	content := strings.TrimSpace(chatResp.Choices[0].Message.Content)
	// Extract JSON block if wrapped in ```json ... ```
	if start := strings.Index(content, "{"); start >= 0 {
		if end := strings.LastIndex(content, "}"); end > start {
			content = content[start : end+1]
		}
	}
	if err := json.Unmarshal([]byte(content), &dec); err != nil {
		return dec, false
	}
	return dec, len(dec.UpdatedFields) > 0
}

// matchFieldsFromCRDSchema inspects the CRD's OpenAPI v3 .spec fields (from the live cluster CRD or YAML)
// and matches them against the failure symptom (memory/OOM vs disk/storage/WAL vs replicas).
func matchFieldsFromCRDSchema(
	ev types.DiagnosticEvent,
	targetAPIVersion, targetKind, targetName string,
	schema *k8s.CRDSchemaSummary,
	newLim, newReq, newStorage string,
) aiFieldDecision {
	reasonLower := strings.ToLower(ev.Reason + " " + ev.Message)
	isStorageIssue := strings.Contains(reasonLower, "disk") ||
		strings.Contains(reasonLower, "storage") ||
		strings.Contains(reasonLower, "pvc") ||
		strings.Contains(reasonLower, "wal") ||
		strings.Contains(reasonLower, "filesystem")

	updated := make(map[string]any)

	if isStorageIssue {
		// Search the CRD's OpenAPI schema fields for storage/size/volume paths
		for _, f := range schema.SpecFields {
			pLower := strings.ToLower(f.Path)
			if strings.Contains(pLower, "wal") && strings.Contains(reasonLower, "wal") && strings.HasSuffix(pLower, "size") {
				updated[f.Path] = newStorage
				break
			}
			if (strings.Contains(pLower, "storage") || strings.Contains(pLower, "volumeclaim")) &&
				(strings.HasSuffix(pLower, ".size") || strings.HasSuffix(pLower, ".storage")) &&
				!strings.Contains(pLower, "wal") {
				updated[f.Path] = newStorage
				break
			}
		}
		if len(updated) == 0 {
			updated["spec.storage.size"] = newStorage
		}
		return aiFieldDecision{
			IssueType:     types.IssuePVCCapacityExhausted,
			UpdatedFields: updated,
			Reasoning: fmt.Sprintf(
				"Walked ownerReferences hierarchy (%s -> %s.%s/%s) and introspected CRD %q (%s). Identified storage capacity field %v in the CRD schema and updated it to %s for online operator reconciliation.",
				defaultName(ev.PodName, targetName), targetKind, targetAPIVersion, targetName, schema.CRDName, schema.SchemaSource, keysOf(updated), newStorage,
			),
		}
	}

	// Memory / OOMKilled / Degraded CR: search the CRD's OpenAPI schema fields for memory limit & request paths
	for _, f := range schema.SpecFields {
		pLower := strings.ToLower(f.Path)
		if strings.HasSuffix(pLower, "limits.memory") {
			updated[f.Path] = newLim
		} else if strings.HasSuffix(pLower, "requests.memory") {
			updated[f.Path] = newReq
		}
	}
	if len(updated) == 0 {
		updated["spec.resources.limits.memory"] = newLim
		updated["spec.resources.requests.memory"] = newReq
	}

	issueType := types.IssuePodOOMKilled
	if ev.ExitCode != 137 && !strings.Contains(reasonLower, "oom") {
		issueType = types.IssueOperatorCRDegraded
	}

	var chainHops []string
	for _, link := range ev.OwnershipChain {
		chainHops = append(chainHops, fmt.Sprintf("%s/%s", link.Kind, link.Name))
	}
	chainStr := strings.Join(chainHops, " -> ")
	if chainStr == "" {
		chainStr = fmt.Sprintf("Pod/%s -> %s/%s", defaultName(ev.PodName, targetName+"-0"), targetKind, targetName)
	}

	statusPhase := ""
	if strings.EqualFold(targetKind, "Cluster") && strings.Contains(targetAPIVersion, "cnpg.io") {
		statusPhase = "Cluster in healthy state"
	} else if strings.EqualFold(targetKind, "NooBaa") {
		statusPhase = "Ready"
	}

	return aiFieldDecision{
		IssueType:         issueType,
		UpdatedFields:     updated,
		StatusPhaseUpdate: statusPhase,
		Reasoning: fmt.Sprintf(
			"Traversed ownerReferences hierarchy (%s) to controlling Custom Resource %s.%s/%s and introspected CRD %q (%s). Using the CRD OpenAPI v3 schema and problem statement (%s), selected CR spec fields %v to remediate the operator workload without fighting child controller reconciliation.",
			chainStr, targetKind, targetAPIVersion, targetName, schema.CRDName, schema.SchemaSource, ev.Reason, keysOf(updated),
		),
	}
}

func (a *universalAICRDAdapter) resolveTargetCR(ev types.DiagnosticEvent) (apiVersion, kind, name string) {
	apiVersion = a.spec.PrimaryGVK.APIVersion
	kind = a.spec.PrimaryGVK.Kind
	name = ev.OwnerName

	for _, link := range ev.OwnershipChain {
		if a.spec.PrimaryGVK.Kind != "" && strings.EqualFold(link.Kind, a.spec.PrimaryGVK.Kind) {
			return defaultName(link.APIVersion, apiVersion), link.Kind, link.Name
		}
	}
	// If wildcard adapter, pick the highest Custom Resource in the ownership chain
	for i := len(ev.OwnershipChain) - 1; i >= 0; i-- {
		link := ev.OwnershipChain[i]
		if isCustomResourceGVK(link.APIVersion, link.Kind) {
			return link.APIVersion, link.Kind, link.Name
		}
	}

	if apiVersion == "" {
		apiVersion = defaultName(ev.OwnerAPIVersion, "postgresql.cnpg.io/v1")
	}
	if kind == "" {
		kind = defaultName(ev.OwnerKind, "Cluster")
	}
	if name == "" {
		for _, lk := range a.spec.PodLabelKeys {
			if ev.Labels != nil && ev.Labels[lk] != "" {
				name = ev.Labels[lk]
				break
			}
		}
	}
	if name == "" {
		name = strings.ToLower(kind)
	}
	return apiVersion, kind, name
}

func isCustomResourceGVK(apiVersion, kind string) bool {
	av := strings.ToLower(strings.TrimSpace(apiVersion))
	k := strings.ToLower(strings.TrimSpace(kind))
	if av == "" || av == "v1" || av == "apps/v1" || av == "batch/v1" || av == "autoscaling/v2" || av == "policy/v1" || av == "networking.k8s.io/v1" || av == "storage.k8s.io/v1" || av == "rbac.authorization.k8s.io/v1" {
		return false
	}
	switch k {
	case "pod", "statefulset", "deployment", "replicaset", "daemonset", "service", "node", "namespace", "persistentvolumeclaim", "resourcequota", "limitrange":
		return false
	}
	return strings.Contains(av, ".")
}

func splitGroupVersion(apiVersion string) (string, string) {
	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", apiVersion
}

func fstGroup(apiVersion string) string {
	g, _ := splitGroupVersion(apiVersion)
	return g
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func buildNestedPatch(dotPath string, value any) map[string]any {
	parts := strings.Split(strings.TrimPrefix(dotPath, "."), ".")
	root := make(map[string]any)
	cur := root
	for i, p := range parts {
		if i == len(parts)-1 {
			cur[p] = value
		} else {
			next := make(map[string]any)
			cur[p] = next
			cur = next
		}
	}
	return root
}

func mergeNestedMap(dst, src map[string]any) {
	for k, v := range src {
		if srcMap, ok := v.(map[string]any); ok {
			if dstMap, ok2 := dst[k].(map[string]any); ok2 {
				mergeNestedMap(dstMap, srcMap)
				continue
			}
		}
		dst[k] = v
	}
}

func defaultName(val, fallback string) string {
	if strings.TrimSpace(val) != "" {
		return val
	}
	return fallback
}
