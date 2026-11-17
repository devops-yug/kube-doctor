package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/luigi/kube-doctor/internal/k8s"
	"github.com/luigi/kube-doctor/internal/types"
)

// GVKDescriptor describes a Custom Resource Group/Version/Kind managed by an Operator.
type GVKDescriptor struct {
	APIVersion string `json:"apiVersion" yaml:"apiVersion"` // e.g., "postgresql.cnpg.io/v1"
	Kind       string `json:"kind" yaml:"kind"`             // e.g., "Cluster"
	Plural     string `json:"plural" yaml:"plural"`         // e.g., "clusters"
	Role       string `json:"role" yaml:"role"`             // "rootController", "subResource", "workload"
}

// AdapterRemediationPlan defines the operator-aware remediation plan produced by the AI + CRD Schema Engine.
type AdapterRemediationPlan struct {
	AdapterName        string                    `json:"adapterName"`
	IssueType          types.IssueType           `json:"issueType"`
	Criticality        types.Criticality         `json:"criticality"`
	Severity           types.Severity            `json:"severity"`
	Summary            string                    `json:"summary"`
	RootCauseAnalysis  string                    `json:"rootCauseAnalysis"`
	K8s135FeaturesUsed []string                  `json:"k8s135FeaturesUsed,omitempty"`
	OCP422FeaturesUsed []string                  `json:"ocp422FeaturesUsed,omitempty"`
	CRDSchemaUsed      *k8s.CRDSchemaSummary     `json:"crdSchemaUsed,omitempty"`
	UpdatedFields      map[string]any            `json:"updatedFields,omitempty"`
	PrimaryAction      types.RemediationAction   `json:"primaryAction"`
	FollowUpActions    []types.RemediationAction `json:"followUpActions,omitempty"`
}

// OperatorAdapter is the interface for dynamic YAML + Live CRD OpenAPI v3 + AI Model Operator Adapters.
type OperatorAdapter interface {
	Name() string
	Description() string
	SourceType() string
	SupportedGVKs() []GVKDescriptor
	MatchesEvent(ev types.DiagnosticEvent) bool
	BuildRemediationPlan(ctx context.Context, k8sClient *k8s.Client, ev types.DiagnosticEvent) (*AdapterRemediationPlan, error)
}

// DeclarativeAdapterSpec allows users to drop in any Operator YAML file in configs/adapters/*.yaml
// (or register via POST /v1/adapters) OR let kube-doctor discover the CRD OpenAPI v3 schema directly from the cluster.
type DeclarativeAdapterSpec struct {
	Name                string               `json:"name" yaml:"name"`
	Description         string               `json:"description" yaml:"description"`
	PrimaryGVK          GVKDescriptor        `json:"primaryGvk" yaml:"primaryGvk"`
	HierarchyGVKs       []GVKDescriptor      `json:"hierarchyGvks,omitempty" yaml:"hierarchyGvks,omitempty"`
	PodLabelKeys        []string             `json:"podLabelKeys,omitempty" yaml:"podLabelKeys,omitempty"`
	StatusPhasePath     string               `json:"statusPhasePath" yaml:"statusPhasePath"`
	AutoRemediateMemory bool                 `json:"autoRemediateMemory" yaml:"autoRemediateMemory"`
	CRDSchemaFields     []k8s.CRDSchemaField `json:"crdSchemaFields,omitempty" yaml:"crdSchemaFields,omitempty"`
}

// Registry manages YAML-loaded Operator Adapter specs and the Universal Live CRD + AI Model Adapter.
// No operator-specific Go structs are hardcoded; every operator is understood via its YAML profile
// and/or live Kubernetes CRD OpenAPI v3 schema + AI model reasoning.
type Registry struct {
	mu       sync.RWMutex
	adapters []OperatorAdapter
	fallback OperatorAdapter
}

// NewRegistry creates an OperatorAdapter Registry that loads all YAML operator profiles from
// configs/adapters/ and attaches a Universal Live-CRD + AI Adapter that can handle ANY Custom Resource
// discovered in the cluster via ownerReferences even if no YAML file was pre-created for it.
func NewRegistry(configDirs ...string) *Registry {
	r := &Registry{
		fallback: NewUniversalAICRDAdapter(DeclarativeAdapterSpec{
			Name:                "universal-crd-ai-adapter",
			Description:         "Universal Kubernetes CRD OpenAPI v3 Schema + AI Model Adapter (dynamically introspects any CustomResourceDefinition in the cluster and uses the LLM to select the exact CR spec field to patch)",
			AutoRemediateMemory: true,
		}),
	}

	// Search standard locations for configs/adapters/*.yaml (working dir, parent dir for tests, /app/configs/adapters in container)
	candidates := []string{"configs/adapters", "../configs/adapters", "../../configs/adapters", "/app/configs/adapters"}
	if len(configDirs) > 0 && configDirs[0] != "" {
		candidates = append([]string{configDirs[0]}, candidates...)
	}
	for _, dir := range candidates {
		if err := r.LoadDeclarativeProfilesFromDir(dir); err == nil && len(r.adapters) > 0 {
			break
		}
	}
	return r
}

// Register adds or replaces an OperatorAdapter by name.
func (r *Registry) Register(adapter OperatorAdapter) {
	if adapter == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, existing := range r.adapters {
		if strings.EqualFold(existing.Name(), adapter.Name()) {
			r.adapters[i] = adapter
			return
		}
	}
	r.adapters = append(r.adapters, adapter)
}

// RegisterDeclarativeSpec registers a YAML/JSON OperatorAdapter backed by the AI + CRD Schema Engine.
func (r *Registry) RegisterDeclarativeSpec(spec DeclarativeAdapterSpec) error {
	if spec.Name == "" || spec.PrimaryGVK.Kind == "" {
		return fmt.Errorf("declarative adapter requires name and primaryGvk.kind")
	}
	r.Register(NewUniversalAICRDAdapter(spec))
	return nil
}

// Match returns the matching YAML-configured OperatorAdapter, OR if the event's ownerReferences
// point to any non-core Custom Resource (e.g. any *.io / *.com CRD in the cluster), returns
// the Universal Live-CRD + AI Adapter so kube-doctor can introspect its CRD from the cluster automatically!
func (r *Registry) Match(ev types.DiagnosticEvent) OperatorAdapter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, a := range r.adapters {
		if a.MatchesEvent(ev) {
			return a
		}
	}
	if r.fallback != nil && r.fallback.MatchesEvent(ev) {
		return r.fallback
	}
	return nil
}

// ListMetadata returns serializable metadata for all registered YAML operator profiles + the Universal CRD AI engine.
func (r *Registry) ListMetadata() []map[string]any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]map[string]any, 0, len(r.adapters)+1)
	for _, a := range r.adapters {
		out = append(out, map[string]any{
			"name":          a.Name(),
			"description":   a.Description(),
			"sourceType":    a.SourceType(),
			"supportedGvks": a.SupportedGVKs(),
		})
	}
	if r.fallback != nil {
		out = append(out, map[string]any{
			"name":          r.fallback.Name(),
			"description":   r.fallback.Description(),
			"sourceType":    r.fallback.SourceType(),
			"supportedGvks": r.fallback.SupportedGVKs(),
		})
	}
	return out
}

// LoadDeclarativeProfilesFromDir loads all *.json and *.yaml operator profiles from a directory.
func (r *Registry) LoadDeclarativeProfilesFromDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".json" && ext != ".yaml" && ext != ".yml" {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		spec, err := parseDeclarativeSpec(raw)
		if err == nil && spec.Name != "" {
			_ = r.RegisterDeclarativeSpec(spec)
		}
	}
	return nil
}

func parseDeclarativeSpec(raw []byte) (DeclarativeAdapterSpec, error) {
	var spec DeclarativeAdapterSpec
	trimmed := strings.TrimSpace(string(raw))
	if strings.HasPrefix(trimmed, "{") {
		err := json.Unmarshal(raw, &spec)
		return spec, err
	}

	spec.AutoRemediateMemory = true
	lines := strings.Split(trimmed, "\n")
	section := ""
	var curField *k8s.CRDSchemaField

	flushField := func() {
		if curField != nil && curField.Path != "" {
			spec.CRDSchemaFields = append(spec.CRDSchemaFields, *curField)
		}
		curField = nil
	}

	for _, line := range lines {
		clean := strings.TrimSpace(line)
		if clean == "" || strings.HasPrefix(clean, "#") {
			continue
		}
		if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") && strings.HasSuffix(clean, ":") {
			flushField()
			section = strings.TrimSuffix(clean, ":")
			continue
		}
		if section == "podLabelKeys" && strings.HasPrefix(clean, "- ") {
			spec.PodLabelKeys = append(spec.PodLabelKeys, strings.Trim(strings.TrimPrefix(clean, "- "), `"' `))
			continue
		}
		if section == "crdSchemaFields" {
			if strings.HasPrefix(clean, "- ") {
				flushField()
				curField = &k8s.CRDSchemaField{}
				clean = strings.TrimSpace(strings.TrimPrefix(clean, "- "))
			}
			if curField != nil {
				parts := strings.SplitN(clean, ":", 2)
				if len(parts) == 2 {
					k := strings.TrimSpace(parts[0])
					v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
					switch k {
					case "path":
						curField.Path = v
					case "type":
						curField.Type = v
					case "description":
						curField.Description = v
					}
				}
			}
			continue
		}

		parts := strings.SplitN(clean, ":", 2)
		if len(parts) != 2 {
			continue
		}
		k := strings.TrimSpace(strings.TrimPrefix(parts[0], "- "))
		v := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		if section == "primaryGvk" {
			switch k {
			case "apiVersion":
				spec.PrimaryGVK.APIVersion = v
			case "kind":
				spec.PrimaryGVK.Kind = v
			case "plural":
				spec.PrimaryGVK.Plural = v
			case "role":
				spec.PrimaryGVK.Role = v
			}
			continue
		}
		switch k {
		case "name":
			spec.Name = v
		case "description":
			spec.Description = v
		case "statusPhasePath":
			spec.StatusPhasePath = v
		case "autoRemediateMemory":
			spec.AutoRemediateMemory = strings.EqualFold(v, "true")
		}
	}
	flushField()
	return spec, nil
}
