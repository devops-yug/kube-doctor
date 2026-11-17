package k8s

import (
	"context"
	"fmt"
	"strings"

	"github.com/luigi/kube-doctor/internal/types"
)

// GenericK8sObject represents an arbitrary Kubernetes or OpenShift Custom Resource
// with metadata, ownerReferences, spec, and status.
type GenericK8sObject struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		UID             string            `json:"uid"`
		Labels          map[string]string `json:"labels"`
		Annotations     map[string]string `json:"annotations"`
		OwnerReferences []struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Name       string `json:"name"`
			UID        string `json:"uid"`
			Controller *bool  `json:"controller"`
		} `json:"ownerReferences"`
	} `json:"metadata"`
	Spec   map[string]any `json:"spec"`
	Status map[string]any `json:"status"`
}

// GenericK8sList represents a list of arbitrary Custom Resources.
type GenericK8sList struct {
	Items []GenericK8sObject `json:"items"`
}

// ResolveOwnershipHierarchy walks metadata.ownerReferences recursively (up to 8 levels deep)
// from any starting Kubernetes resource (e.g., Pod, StatefulSet, NooBaa, Cluster.postgresql.cnpg.io)
// up to the root controlling Custom Resource, and also discovers related/child CRs in the same
// operator hierarchy (such as NooBaa's 5 CRs: StorageCluster -> NooBaa -> BackingStore / BucketClass / NamespaceStore,
// or CloudNativePG's Cluster -> Pooler / Backup / ScheduledBackup).
func (c *Client) ResolveOwnershipHierarchy(ctx context.Context, namespace, seedAPIVersion, seedKind, seedName string) (*types.ResourceHierarchyTree, error) {
	if namespace == "" {
		namespace = "default"
	}
	if seedAPIVersion == "" {
		seedAPIVersion = inferDefaultAPIVersion(seedKind)
	}

	tree := &types.ResourceHierarchyTree{
		Namespace:      namespace,
		SeedAPIVersion: seedAPIVersion,
		SeedKind:       seedKind,
		SeedName:       seedName,
	}

	if c.simulate {
		return simulateHierarchyTree(namespace, seedAPIVersion, seedKind, seedName), nil
	}

	visited := make(map[string]bool)
	curAPIVersion := seedAPIVersion
	curKind := seedKind
	curName := seedName
	curNS := namespace

	for depth := 0; depth < 8; depth++ {
		key := fmt.Sprintf("%s/%s/%s/%s", curAPIVersion, curKind, curNS, curName)
		if visited[key] {
			break
		}
		visited[key] = true

		var obj GenericK8sObject
		apiPath := buildResourcePath(curAPIVersion, curKind, curNS, curName)
		if err := c.GetJSON(ctx, apiPath, &obj); err != nil {
			// If depth == 0 and the caller didn't specify exact kind (e.g. Pod vs CR), try fallback
			if depth == 0 && strings.EqualFold(curKind, "Pod") {
				// Maybe seedName is actually a CNPG Cluster or StatefulSet
				if err2 := c.GetJSON(ctx, buildResourcePath("postgresql.cnpg.io/v1", "Cluster", curNS, curName), &obj); err2 == nil {
					curAPIVersion = "postgresql.cnpg.io/v1"
					curKind = "Cluster"
				} else {
					break
				}
			} else {
				break
			}
		}

		if obj.APIVersion != "" {
			curAPIVersion = obj.APIVersion
		}
		if obj.Kind != "" {
			curKind = obj.Kind
		}

		link := types.OwnerChainLink{
			Depth:      depth,
			APIVersion: curAPIVersion,
			Kind:       curKind,
			Name:       curName,
			Namespace:  curNS,
			UID:        obj.Metadata.UID,
			Controller: depth > 0,
			Relation:   "self",
			Phase:      extractStatusPhase(obj.Status),
			Conditions: extractStatusConditions(obj.Status),
			SpecFields: extractKeySpecFields(curKind, obj.Spec, obj.Status),
		}
		if depth > 0 {
			link.Relation = "ownerReference"
		}
		tree.OwnershipChain = append(tree.OwnershipChain, link)

		// Pick next parent from metadata.ownerReferences (preferring controller=true)
		var nextAPIVersion, nextKind, nextName string
		for _, ref := range obj.Metadata.OwnerReferences {
			if ref.Controller != nil && *ref.Controller {
				nextAPIVersion = ref.APIVersion
				nextKind = ref.Kind
				nextName = ref.Name
				break
			}
		}
		if nextName == "" && len(obj.Metadata.OwnerReferences) > 0 {
			ref := obj.Metadata.OwnerReferences[0]
			nextAPIVersion = ref.APIVersion
			nextKind = ref.Kind
			nextName = ref.Name
		}

		// Fallback via operator labels when an operator manages pods via labels without ownerReferences
		if nextName == "" && obj.Metadata.Labels != nil {
			if cnpgCluster := obj.Metadata.Labels["cnpg.io/cluster"]; cnpgCluster != "" && !strings.EqualFold(curKind, "Cluster") {
				nextAPIVersion = "postgresql.cnpg.io/v1"
				nextKind = "Cluster"
				nextName = cnpgCluster
			} else if noobaaApp := obj.Metadata.Labels["noobaa-core"]; noobaaApp != "" && !strings.EqualFold(curKind, "NooBaa") {
				nextAPIVersion = "noobaa.io/v1alpha1"
				nextKind = "NooBaa"
				nextName = "noobaa"
			} else if strimziCluster := obj.Metadata.Labels["strimzi.io/cluster"]; strimziCluster != "" && !strings.EqualFold(curKind, "Kafka") {
				nextAPIVersion = "kafka.strimzi.io/v1beta2"
				nextKind = "Kafka"
				nextName = strimziCluster
			}
		}

		if nextName == "" {
			break
		}
		curAPIVersion = nextAPIVersion
		curKind = nextKind
		curName = nextName
	}

	if len(tree.OwnershipChain) > 0 {
		root := tree.OwnershipChain[len(tree.OwnershipChain)-1]
		tree.RootController = &root
		tree.OperatorAdapter = detectOperatorAdapterFromChain(tree.OwnershipChain)
		tree.RelatedCRs = c.discoverRelatedOperatorCRs(ctx, namespace, tree.OwnershipChain)
	}

	return tree, nil
}

// discoverRelatedOperatorCRs inspects sibling/child Custom Resources that belong to the same operator
// ecosystem (e.g. NooBaa's BackingStore, BucketClass, NamespaceStore, CephObjectStore, or CNPG's Pooler, Backup, ScheduledBackup).
func (c *Client) discoverRelatedOperatorCRs(ctx context.Context, namespace string, chain []types.OwnerChainLink) []types.OwnerChainLink {
	var related []types.OwnerChainLink
	adapter := detectOperatorAdapterFromChain(chain)

	switch adapter {
	case "cloudnative-pg":
		var clusterName string
		for _, link := range chain {
			if strings.EqualFold(link.Kind, "Cluster") {
				clusterName = link.Name
				break
			}
		}
		for _, gvk := range []struct{ apiVersion, kind string }{
			{"postgresql.cnpg.io/v1", "Pooler"},
			{"postgresql.cnpg.io/v1", "Backup"},
			{"postgresql.cnpg.io/v1", "ScheduledBackup"},
		} {
			var list GenericK8sList
			if err := c.GetJSON(ctx, BuildResourceListPath(gvk.apiVersion, gvk.kind, namespace), &list); err == nil {
				for _, item := range list.Items {
					clusterRef, _ := nestedString(item.Spec, "cluster", "name")
					if clusterName == "" || clusterRef == clusterName || strings.HasPrefix(item.Metadata.Name, clusterName) {
						related = append(related, types.OwnerChainLink{
							APIVersion: gvk.apiVersion,
							Kind:       gvk.kind,
							Name:       item.Metadata.Name,
							Namespace:  namespace,
							Relation:   "specReference(cluster.name)",
							Phase:      extractStatusPhase(item.Status),
							Conditions: extractStatusConditions(item.Status),
							SpecFields: extractKeySpecFields(gvk.kind, item.Spec, item.Status),
						})
					}
				}
			}
		}

	case "noobaa-odf":
		// NooBaa / OpenShift Data Foundation 5-CR ecosystem:
		// StorageCluster (ocs.openshift.io/v1) -> NooBaa (noobaa.io/v1alpha1) -> BackingStore, BucketClass, NamespaceStore
		for _, gvk := range []struct{ apiVersion, kind string }{
			{"ocs.openshift.io/v1", "StorageCluster"},
			{"noobaa.io/v1alpha1", "NooBaa"},
			{"noobaa.io/v1alpha1", "BackingStore"},
			{"noobaa.io/v1alpha1", "BucketClass"},
			{"noobaa.io/v1alpha1", "NamespaceStore"},
		} {
			var list GenericK8sList
			if err := c.GetJSON(ctx, BuildResourceListPath(gvk.apiVersion, gvk.kind, namespace), &list); err == nil {
				for _, item := range list.Items {
					alreadyInChain := false
					for _, link := range chain {
						if strings.EqualFold(link.Kind, gvk.kind) && link.Name == item.Metadata.Name {
							alreadyInChain = true
							break
						}
					}
					if alreadyInChain {
						continue
					}
					related = append(related, types.OwnerChainLink{
						APIVersion: gvk.apiVersion,
						Kind:       gvk.kind,
						Name:       item.Metadata.Name,
						Namespace:  namespace,
						Relation:   "noobaaHierarchyCR",
						Phase:      extractStatusPhase(item.Status),
						Conditions: extractStatusConditions(item.Status),
						SpecFields: extractKeySpecFields(gvk.kind, item.Spec, item.Status),
					})
				}
			}
		}
	}

	return related
}

func detectOperatorAdapterFromChain(chain []types.OwnerChainLink) string {
	for _, link := range chain {
		av := strings.ToLower(link.APIVersion)
		k := strings.ToLower(link.Kind)
		switch {
		case strings.Contains(av, "postgresql.cnpg.io") || (k == "cluster" && strings.Contains(av, "cnpg")):
			return "cloudnative-pg"
		case strings.Contains(av, "noobaa.io") || strings.Contains(av, "ocs.openshift.io") || k == "noobaa" || k == "backingstore" || k == "bucketclass" || k == "namespacestore":
			return "noobaa-odf"
		case strings.Contains(av, "strimzi.io") || k == "kafka":
			return "strimzi-kafka"
		case strings.Contains(av, "monitoring.coreos.com"):
			return "prometheus-operator"
		case strings.Contains(av, "argoproj.io"):
			return "argocd-operator"
		}
	}
	return ""
}

func inferDefaultAPIVersion(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "pod", "service", "persistentvolumeclaim", "resourcequota", "limitrange", "node", "namespace":
		return "v1"
	case "statefulset", "deployment", "daemonset", "replicaset":
		return "apps/v1"
	case "cluster", "pooler", "backup", "scheduledbackup":
		return "postgresql.cnpg.io/v1"
	case "noobaa", "backingstore", "bucketclass", "namespacestore":
		return "noobaa.io/v1alpha1"
	case "storagecluster":
		return "ocs.openshift.io/v1"
	case "kafka":
		return "kafka.strimzi.io/v1beta2"
	default:
		return "v1"
	}
}

func extractStatusPhase(status map[string]any) string {
	if status == nil {
		return ""
	}
	if p, ok := status["phase"].(string); ok && p != "" {
		return p
	}
	if s, ok := status["state"].(string); ok && s != "" {
		return s
	}
	return ""
}

func extractStatusConditions(status map[string]any) []string {
	if status == nil {
		return nil
	}
	rawConds, ok := status["conditions"].([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, item := range rawConds {
		cm, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cType, _ := cm["type"].(string)
		cStatus, _ := cm["status"].(string)
		cReason, _ := cm["reason"].(string)
		cMsg, _ := cm["message"].(string)
		if cType != "" {
			summary := fmt.Sprintf("%s=%s", cType, cStatus)
			if cReason != "" {
				summary += fmt.Sprintf(" (%s)", cReason)
			}
			if cMsg != "" && (cStatus == "False" || strings.Contains(strings.ToLower(cType), "degraded") || strings.Contains(strings.ToLower(cType), "fail")) {
				summary += ": " + cMsg
			}
			out = append(out, summary)
		}
	}
	return out
}

func extractKeySpecFields(kind string, spec, status map[string]any) map[string]string {
	fields := make(map[string]string)
	if spec != nil {
		if v, ok := nestedString(spec, "resources", "limits", "memory"); ok {
			fields["memoryLimit"] = v
		}
		if v, ok := nestedString(spec, "resources", "requests", "memory"); ok {
			fields["memoryRequest"] = v
		}
		if v, ok := nestedString(spec, "storage", "size"); ok {
			fields["storageSize"] = v
		}
		if v, ok := nestedString(spec, "walStorage", "size"); ok {
			fields["walStorageSize"] = v
		}
		if v, ok := spec["instances"]; ok {
			fields["instances"] = fmt.Sprintf("%v", v)
		}
		if v, ok := spec["replicas"]; ok {
			fields["replicas"] = fmt.Sprintf("%v", v)
		}
		if v, ok := nestedString(spec, "dbResources", "limits", "memory"); ok {
			fields["dbMemoryLimit"] = v
		}
		if v, ok := nestedString(spec, "coreResources", "limits", "memory"); ok {
			fields["coreMemoryLimit"] = v
		}
		if v, ok := nestedString(spec, "defaultBackingStore"); ok {
			fields["defaultBackingStore"] = v
		}
	}
	if status != nil {
		if v, ok := status["currentPrimary"].(string); ok && v != "" {
			fields["currentPrimary"] = v
		}
		if v, ok := status["readyInstances"]; ok {
			fields["readyInstances"] = fmt.Sprintf("%v", v)
		}
	}
	if len(fields) == 0 {
		return nil
	}
	return fields
}

func nestedString(m map[string]any, keys ...string) (string, bool) {
	cur := any(m)
	for _, k := range keys {
		asMap, ok := cur.(map[string]any)
		if !ok {
			return "", false
		}
		cur, ok = asMap[k]
		if !ok {
			return "", false
		}
	}
	if s, ok := cur.(string); ok && s != "" {
		return s, true
	}
	return "", false
}

func simulateHierarchyTree(namespace, seedAPIVersion, seedKind, seedName string) *types.ResourceHierarchyTree {
	nameLower := strings.ToLower(seedName + " " + seedKind)
	if strings.Contains(nameLower, "noobaa") || strings.Contains(nameLower, "ocs") || strings.Contains(nameLower, "backingstore") {
		chain := []types.OwnerChainLink{
			{
				Depth:      0,
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "noobaa-core-0",
				Namespace:  namespace,
				Relation:   "self",
				Phase:      "Running",
			},
			{
				Depth:      1,
				APIVersion: "apps/v1",
				Kind:       "StatefulSet",
				Name:       "noobaa-core",
				Namespace:  namespace,
				Controller: true,
				Relation:   "ownerReference",
				SpecFields: map[string]string{"replicas": "1"},
			},
			{
				Depth:      2,
				APIVersion: "noobaa.io/v1alpha1",
				Kind:       "NooBaa",
				Name:       "noobaa",
				Namespace:  namespace,
				Controller: true,
				Relation:   "ownerReference",
				Phase:      "Rejected",
				Conditions: []string{"Available=False (BackingStorePhaseRejected): BackingStore noobaa-pv-backing-store is in Rejected phase"},
				SpecFields: map[string]string{"coreMemoryLimit": "4Gi", "dbMemoryLimit": "4Gi"},
			},
			{
				Depth:      3,
				APIVersion: "ocs.openshift.io/v1",
				Kind:       "StorageCluster",
				Name:       "ocs-storagecluster",
				Namespace:  namespace,
				Controller: true,
				Relation:   "ownerReference",
				Phase:      "Degraded",
				Conditions: []string{"Degraded=True (NooBaaDegraded): Multi-Cloud Object Gateway (NooBaa) reconciliation degraded"},
			},
		}
		root := chain[len(chain)-1]
		return &types.ResourceHierarchyTree{
			Namespace:       namespace,
			SeedAPIVersion:  seedAPIVersion,
			SeedKind:        seedKind,
			SeedName:        seedName,
			OperatorAdapter: "noobaa-odf",
			RootController:  &root,
			OwnershipChain:  chain,
			RelatedCRs: []types.OwnerChainLink{
				{
					APIVersion: "noobaa.io/v1alpha1",
					Kind:       "BackingStore",
					Name:       "noobaa-pv-backing-store",
					Namespace:  namespace,
					Relation:   "noobaaHierarchyCR",
					Phase:      "Rejected",
					Conditions: []string{"Available=False (InsufficientTargetPodMemory): BackingStore target pod OOMKilled or PVC pending"},
				},
				{
					APIVersion: "noobaa.io/v1alpha1",
					Kind:       "BucketClass",
					Name:       "noobaa-default-bucket-class",
					Namespace:  namespace,
					Relation:   "noobaaHierarchyCR",
					Phase:      "Verifying",
					SpecFields: map[string]string{"defaultBackingStore": "noobaa-pv-backing-store"},
				},
				{
					APIVersion: "noobaa.io/v1alpha1",
					Kind:       "NamespaceStore",
					Name:       "noobaa-ns-store",
					Namespace:  namespace,
					Relation:   "noobaaHierarchyCR",
					Phase:      "Ready",
				},
			},
		}
	}

	chain := []types.OwnerChainLink{
		{
			Depth:      0,
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       seedName,
			Namespace:  namespace,
			Relation:   "self",
			Phase:      "CrashLoopBackOff",
		},
		{
			Depth:      1,
			APIVersion: "postgresql.cnpg.io/v1",
			Kind:       "Cluster",
			Name:       "pg-cluster",
			Namespace:  namespace,
			Controller: true,
			Relation:   "ownerReference",
			Phase:      "Failing",
			Conditions: []string{"Ready=False (InstanceOOMKilled): Pod pg-cluster-1 terminated with OOMKilled (ExitCode 137)"},
			SpecFields: map[string]string{
				"instances":      "3",
				"memoryLimit":    "256Mi",
				"memoryRequest":  "128Mi",
				"storageSize":    "10Gi",
				"currentPrimary": "pg-cluster-1",
			},
		},
	}
	root := chain[len(chain)-1]
	return &types.ResourceHierarchyTree{
		Namespace:       namespace,
		SeedAPIVersion:  seedAPIVersion,
		SeedKind:        seedKind,
		SeedName:        seedName,
		OperatorAdapter: "cloudnative-pg",
		RootController:  &root,
		OwnershipChain:  chain,
		RelatedCRs: []types.OwnerChainLink{
			{
				APIVersion: "postgresql.cnpg.io/v1",
				Kind:       "Pooler",
				Name:       "pg-cluster-pooler-rw",
				Namespace:  namespace,
				Relation:   "specReference(cluster.name)",
				Phase:      "Healthy",
			},
			{
				APIVersion: "postgresql.cnpg.io/v1",
				Kind:       "ScheduledBackup",
				Name:       "pg-cluster-nightly",
				Namespace:  namespace,
				Relation:   "specReference(cluster.name)",
				Phase:      "Active",
			},
		},
	}
}
