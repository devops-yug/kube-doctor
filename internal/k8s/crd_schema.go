package k8s

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// CRDSchemaField represents a single flattened field path extracted from a Kubernetes
// CustomResourceDefinition's OpenAPI v3 schema (or from a live Custom Resource's .spec).
type CRDSchemaField struct {
	Path        string `json:"path"`                  // e.g. "spec.resources.limits.memory", "spec.storage.size"
	Type        string `json:"type"`                  // e.g. "string", "integer", "object"
	Description string `json:"description,omitempty"` // OpenAPI v3 description from the CRD
}

// CRDSchemaSummary holds the introspected OpenAPI v3 schema for a CustomResourceDefinition
// along with the live Custom Resource instance's current .spec and .status.
type CRDSchemaSummary struct {
	CRDName        string           `json:"crdName"`        // e.g. "clusters.postgresql.cnpg.io"
	Group          string           `json:"group"`          // e.g. "postgresql.cnpg.io"
	Version        string           `json:"version"`        // e.g. "v1"
	Kind           string           `json:"kind"`           // e.g. "Cluster"
	Plural         string           `json:"plural"`         // e.g. "clusters"
	Scope          string           `json:"scope"`          // "Namespaced" or "Cluster"
	SpecFields     []CRDSchemaField `json:"specFields"`     // Relevant .spec fields extracted from CRD openAPIV3Schema
	LiveCRSpec     map[string]any   `json:"liveCrSpec,omitempty"`
	LiveCRStatus   map[string]any   `json:"liveCrStatus,omitempty"`
	SchemaSource   string           `json:"schemaSource"`   // "live-cluster-crd", "live-cr-introspection", or "yaml-catalog"
}

// InspectCRDSchemaAndInstance queries the live Kubernetes cluster for:
//  1. The CustomResourceDefinition (/apis/apiextensions.k8s.io/v1/customresourcedefinitions/{plural}.{group})
//     to extract its OpenAPI v3 .spec field paths and descriptions.
//  2. The live Custom Resource instance (/apis/{group}/{version}/namespaces/{ns}/{plural}/{name})
//     to read its current .spec and .status values.
func (c *Client) InspectCRDSchemaAndInstance(ctx context.Context, apiVersion, kind, namespace, name string) (*CRDSchemaSummary, error) {
	group, version := splitAPIVersion(apiVersion)
	plural := PluralizeKind(kind)
	crdName := plural
	if group != "" {
		crdName = fmt.Sprintf("%s.%s", plural, group)
	}

	summary := &CRDSchemaSummary{
		CRDName: crdName,
		Group:   group,
		Version: version,
		Kind:    kind,
		Plural:  plural,
		Scope:   "Namespaced",
	}

	if !c.simulate {
		// 1. Fetch the live Custom Resource instance (.spec and .status)
		if name != "" {
			var liveObj GenericK8sObject
			crPath := buildResourcePath(apiVersion, kind, namespace, name)
			if err := c.GetJSON(ctx, crPath, &liveObj); err == nil {
				summary.LiveCRSpec = liveObj.Spec
				summary.LiveCRStatus = liveObj.Status
			}
		}

		// 2. Fetch the live CRD OpenAPI v3 Schema from /apis/apiextensions.k8s.io/v1/customresourcedefinitions/{crdName}
		var crdObj struct {
			Spec struct {
				Group string `json:"group"`
				Scope string `json:"scope"`
				Names struct {
					Kind   string `json:"kind"`
					Plural string `json:"plural"`
				} `json:"names"`
				Versions []struct {
					Name    string `json:"name"`
					Served  bool   `json:"served"`
					Storage bool   `json:"storage"`
					Schema  struct {
						OpenAPIV3Schema map[string]any `json:"openAPIV3Schema"`
					} `json:"schema"`
				} `json:"versions"`
			} `json:"spec"`
		}
		crdPath := fmt.Sprintf("/apis/apiextensions.k8s.io/v1/customresourcedefinitions/%s", crdName)
		if err := c.GetJSON(ctx, crdPath, &crdObj); err == nil {
			if crdObj.Spec.Scope != "" {
				summary.Scope = crdObj.Spec.Scope
			}
			for _, ver := range crdObj.Spec.Versions {
				if ver.Name == version || ver.Storage || len(crdObj.Spec.Versions) == 1 {
					fields := extractOpenAPISpecFields(ver.Schema.OpenAPIV3Schema)
					if len(fields) > 0 {
						summary.SpecFields = fields
						summary.SchemaSource = "live-cluster-crd"
						break
					}
				}
			}
		}
	}

	// 3. If the CRD uses x-kubernetes-preserve-unknown-fields: true (minimal schema) or simulation mode,
	// introspect the live CR .spec structure + any YAML CRD definitions in configs/adapters/
	if len(summary.SpecFields) == 0 && len(summary.LiveCRSpec) > 0 {
		summary.SpecFields = extractFieldsFromLiveSpecMap("spec", summary.LiveCRSpec)
		summary.SchemaSource = "live-cr-introspection"
	}

	return summary, nil
}

// extractOpenAPISpecFields walks an OpenAPI v3 schema under properties.spec and returns
// tunable resource, storage, replica, and configuration field paths for the LLM.
func extractOpenAPISpecFields(rootSchema map[string]any) []CRDSchemaField {
	if rootSchema == nil {
		return nil
	}
	props, _ := rootSchema["properties"].(map[string]any)
	specProp, _ := props["spec"].(map[string]any)
	if specProp == nil {
		return nil
	}

	var fields []CRDSchemaField
	walkOpenAPINode("spec", specProp, 0, &fields)

	// Prioritize fields relevant to remediation (memory, cpu, resources, storage, disk, wal, size, replicas, instances, pool)
	sort.SliceStable(fields, func(i, j int) bool {
		return scoreFieldRelevance(fields[i].Path) > scoreFieldRelevance(fields[j].Path)
	})
	if len(fields) > 60 {
		fields = fields[:60]
	}
	return fields
}

func walkOpenAPINode(prefix string, node map[string]any, depth int, out *[]CRDSchemaField) {
	if depth > 6 || node == nil {
		return
	}
	typ, _ := node["type"].(string)
	desc, _ := node["description"].(string)
	if len(desc) > 160 {
		desc = desc[:160] + "..."
	}

	props, hasProps := node["properties"].(map[string]any)
	// Handle Kubernetes ResourceRequirements (requests/limits map[string]Quantity)
	if strings.HasSuffix(strings.ToLower(prefix), "resources") && (!hasProps || len(props) == 0) {
		*out = append(*out,
			CRDSchemaField{Path: prefix + ".limits.memory", Type: "string (Quantity)", Description: "Container memory limit (e.g. 512Mi, 2Gi). " + desc},
			CRDSchemaField{Path: prefix + ".requests.memory", Type: "string (Quantity)", Description: "Container memory request (e.g. 256Mi, 1Gi). " + desc},
			CRDSchemaField{Path: prefix + ".limits.cpu", Type: "string (Quantity)", Description: "Container CPU limit (e.g. 1000m, 2). " + desc},
		)
		return
	}

	if strings.HasSuffix(prefix, ".limits") || strings.HasSuffix(prefix, ".requests") {
		if !hasProps {
			*out = append(*out,
				CRDSchemaField{Path: prefix + ".memory", Type: "string (Quantity)", Description: "Memory quantity (e.g. 512Mi, 2Gi). " + desc},
				CRDSchemaField{Path: prefix + ".cpu", Type: "string (Quantity)", Description: "CPU quantity (e.g. 500m, 2). " + desc},
			)
			return
		}
	}

	if !hasProps {
		if prefix != "spec" {
			if typ == "" {
				typ = "any"
			}
			*out = append(*out, CRDSchemaField{
				Path:        prefix,
				Type:        typ,
				Description: desc,
			})
		}
		return
	}

	keys := make([]string, 0, len(props))
	for k := range props {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		child, ok := props[k].(map[string]any)
		if !ok {
			continue
		}
		walkOpenAPINode(prefix+"."+k, child, depth+1, out)
	}
}

func extractFieldsFromLiveSpecMap(prefix string, m map[string]any) []CRDSchemaField {
	var out []CRDSchemaField
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := m[k]
		path := prefix + "." + k
		switch typed := v.(type) {
		case map[string]any:
			out = append(out, extractFieldsFromLiveSpecMap(path, typed)...)
		case string:
			out = append(out, CRDSchemaField{Path: path, Type: "string", Description: fmt.Sprintf("Current value: %q", typed)})
		case float64, int, int64:
			out = append(out, CRDSchemaField{Path: path, Type: "integer", Description: fmt.Sprintf("Current value: %v", typed)})
		case bool:
			out = append(out, CRDSchemaField{Path: path, Type: "boolean", Description: fmt.Sprintf("Current value: %v", typed)})
		}
	}
	return out
}

func scoreFieldRelevance(path string) int {
	pl := strings.ToLower(path)
	score := 0
	for _, kw := range []string{"memory", "resources", "limits", "requests", "storage", "size", "wal", "instances", "replicas", "cpu", "pool"} {
		if strings.Contains(pl, kw) {
			score += 10
		}
	}
	return score
}

func splitAPIVersion(apiVersion string) (group, version string) {
	parts := strings.SplitN(strings.TrimSpace(apiVersion), "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

