package k8s

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/luigi/kube-doctor/internal/types"
)

type k8sPodList struct {
	Items []k8sPodItem `json:"items"`
}

type k8sOwnerRef struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
}

type k8sPodItem struct {
	Metadata struct {
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		Labels            map[string]string `json:"labels"`
		DeletionTimestamp string            `json:"deletionTimestamp"`
		OwnerReferences   []k8sOwnerRef     `json:"ownerReferences"`
	} `json:"metadata"`
	Spec struct {
		NodeName   string `json:"nodeName"`
		Containers []struct {
			Name      string `json:"name"`
			Resources struct {
				Limits   map[string]string `json:"limits"`
				Requests map[string]string `json:"requests"`
			} `json:"resources"`
		} `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase             string               `json:"phase"`
		Reason            string               `json:"reason"`
		Message           string               `json:"message"`
		ContainerStatuses []k8sContainerStatus `json:"containerStatuses"`
	} `json:"status"`
}

type k8sContainerStatus struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int    `json:"restartCount"`
	State        struct {
		Waiting *struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"waiting"`
		Terminated *struct {
			ExitCode int    `json:"exitCode"`
			Reason   string `json:"reason"`
			Message  string `json:"message"`
		} `json:"terminated"`
	} `json:"state"`
	LastState struct {
		Terminated *struct {
			ExitCode int    `json:"exitCode"`
			Reason   string `json:"reason"`
			Message  string `json:"message"`
		} `json:"terminated"`
	} `json:"lastState"`
}

type k8sResourceQuotaList struct {
	Items []k8sResourceQuotaItem `json:"items"`
}

type k8sResourceQuotaItem struct {
	Metadata struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Status struct {
		Hard map[string]string `json:"hard"`
		Used map[string]string `json:"used"`
	} `json:"status"`
}

type k8sNodeList struct {
	Items []struct {
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Status struct {
			Conditions []struct {
				Type    string `json:"type"`
				Status  string `json:"status"`
				Reason  string `json:"reason"`
				Message string `json:"message"`
			} `json:"conditions"`
		} `json:"status"`
	} `json:"items"`
}

type k8sPVCList struct {
	Items []struct {
		Metadata struct {
			Name      string `json:"name"`
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Spec struct {
			Resources struct {
				Requests map[string]string `json:"requests"`
			} `json:"resources"`
		} `json:"spec"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

// ScanClusterHealth queries the live Kubernetes API server for unhealthy Pods,
// Nodes, PVCs, and ResourceQuotas across all user namespaces and builds enriched DiagnosticEvents.
func (c *Client) ScanClusterHealth(ctx context.Context) ([]types.DiagnosticEvent, error) {
	if c.simulate {
		return nil, nil
	}

	clusterVer := c.detectClusterVersion(ctx)
	var events []types.DiagnosticEvent

	// 1. Scan Pods across all namespaces
	var podList k8sPodList
	if err := c.GetJSON(ctx, "/api/v1/pods", &podList); err == nil {
		for _, pod := range podList.Items {
			ns := pod.Metadata.Namespace
			if isSystemNamespace(ns) {
				continue
			}

			// Evicted pods
			if strings.EqualFold(pod.Status.Reason, "Evicted") {
				ownerKind, ownerName := c.resolveOwner(ctx, ns, pod.Metadata.OwnerReferences, pod.Metadata.Name)
				ev := types.DiagnosticEvent{
					ID:             fmt.Sprintf("evicted:%s/%s", ns, pod.Metadata.Name),
					ClusterVersion: clusterVer,
					Namespace:      ns,
					PodName:        pod.Metadata.Name,
					OwnerKind:      ownerKind,
					OwnerName:      ownerName,
					NodeName:       pod.Spec.NodeName,
					Reason:         "Evicted",
					Message:        pod.Status.Message,
					Labels:         pod.Metadata.Labels,
					Timestamp:      time.Now().UTC(),
				}
				c.populateOwnershipChainAndQuota(ctx, ns, &ev)
				events = append(events, ev)
				continue
			}

			for _, cs := range pod.Status.ContainerStatuses {
				curLim, curReq := extractContainerMemory(pod, cs.Name)
				ownerKind, ownerName := c.resolveOwner(ctx, ns, pod.Metadata.OwnerReferences, pod.Metadata.Name)

				// Check if pod is OOMKilled (either currently terminated with OOMKilled/137, or in CrashLoopBackOff/unready after OOMKilled/137)
				isOOM := false
				exitCode := 0
				oomMsg := ""

				if cs.State.Terminated != nil && (cs.State.Terminated.Reason == "OOMKilled" || cs.State.Terminated.ExitCode == 137) {
					isOOM = true
					exitCode = 137
					oomMsg = cs.State.Terminated.Message
				} else if cs.LastState.Terminated != nil && (cs.LastState.Terminated.Reason == "OOMKilled" || cs.LastState.Terminated.ExitCode == 137) {
					// Only treat LastState OOMKilled as active if the container is currently not Ready or is waiting in CrashLoopBackOff
					if !cs.Ready || cs.State.Waiting != nil {
						isOOM = true
						exitCode = 137
						oomMsg = cs.LastState.Terminated.Message
					}
				}

				if isOOM {
					// Check live workload template limit in case the workload was already patched
					workloadLim, workloadReq := c.getWorkloadMemory(ctx, ns, ownerKind, ownerName, cs.Name)
					if workloadLim != "" {
						curLim = workloadLim
					}
					if workloadReq != "" {
						curReq = workloadReq
					}

					ev := types.DiagnosticEvent{
						ID:               fmt.Sprintf("oom:%s/%s/%s:%s", ns, ownerKind, ownerName, curLim),
						ClusterVersion:   clusterVer,
						Namespace:        ns,
						PodName:          pod.Metadata.Name,
						ContainerName:    cs.Name,
						OwnerKind:        ownerKind,
						OwnerName:        ownerName,
						NodeName:         pod.Spec.NodeName,
						ExitCode:         exitCode,
						Reason:           "OOMKilled",
						Message:          fmt.Sprintf("Container %s in pod %s/%s terminated with OOMKilled (exitCode=137, restarts=%d, currentLimit=%s). %s", cs.Name, ns, pod.Metadata.Name, cs.RestartCount, curLim, oomMsg),
						CurrentMemoryLim: curLim,
						CurrentMemoryReq: curReq,
						Labels:           pod.Metadata.Labels,
						Timestamp:        time.Now().UTC(),
					}
					c.populateOwnershipChainAndQuota(ctx, ns, &ev)
					// Recompute ID in case ownership chain promoted OwnerKind/OwnerName to a parent Custom Resource (e.g. Cluster/pg-cluster)
					ev.ID = fmt.Sprintf("oom:%s/%s/%s:%s", ns, ev.OwnerKind, ev.OwnerName, ev.CurrentMemoryLim)
					events = append(events, ev)
					continue
				}

				// Check Waiting states: ImagePullBackOff / ErrImagePull / CrashLoopBackOff
				if cs.State.Waiting != nil {
					wReason := cs.State.Waiting.Reason
					switch wReason {
					case "ImagePullBackOff", "ErrImagePull":
						ev := types.DiagnosticEvent{
							ID:             fmt.Sprintf("imgpull:%s/%s/%s", ns, pod.Metadata.Name, cs.Name),
							ClusterVersion: clusterVer,
							Namespace:      ns,
							PodName:        pod.Metadata.Name,
							ContainerName:  cs.Name,
							OwnerKind:      ownerKind,
							OwnerName:      ownerName,
							Reason:         wReason,
							Message:        cs.State.Waiting.Message,
							Labels:         pod.Metadata.Labels,
							Timestamp:      time.Now().UTC(),
						}
						c.populateOwnershipChainAndQuota(ctx, ns, &ev)
						events = append(events, ev)

					case "CrashLoopBackOff":
						ev := types.DiagnosticEvent{
							ID:               fmt.Sprintf("crashloop:%s/%s/%s:%d", ns, ownerKind, ownerName, cs.RestartCount),
							ClusterVersion:   clusterVer,
							Namespace:        ns,
							PodName:          pod.Metadata.Name,
							ContainerName:    cs.Name,
							OwnerKind:        ownerKind,
							OwnerName:        ownerName,
							Reason:           "CrashLoopBackOff",
							Message:          cs.State.Waiting.Message,
							CurrentMemoryLim: curLim,
							CurrentMemoryReq: curReq,
							Labels:           pod.Metadata.Labels,
							Timestamp:        time.Now().UTC(),
						}
						c.populateOwnershipChainAndQuota(ctx, ns, &ev)
						events = append(events, ev)
					}
				}
			}
		}
	}

	// 2. Scan Nodes for NotReady
	var nodeList k8sNodeList
	if err := c.GetJSON(ctx, "/api/v1/nodes", &nodeList); err == nil {
		for _, n := range nodeList.Items {
			for _, cond := range n.Status.Conditions {
				if cond.Type == "Ready" && cond.Status != "True" {
					events = append(events, types.DiagnosticEvent{
						ID:             fmt.Sprintf("node-notready:%s", n.Metadata.Name),
						ClusterVersion: clusterVer,
						NodeName:       n.Metadata.Name,
						Reason:         "NodeNotReady",
						Message:        fmt.Sprintf("Node %s Ready=%s (%s: %s)", n.Metadata.Name, cond.Status, cond.Reason, cond.Message),
						Timestamp:      time.Now().UTC(),
					})
				}
			}
		}
	}

	// 3. Scan PVCs for Pending
	var pvcList k8sPVCList
	if err := c.GetJSON(ctx, "/api/v1/persistentvolumeclaims", &pvcList); err == nil {
		for _, pvc := range pvcList.Items {
			if isSystemNamespace(pvc.Metadata.Namespace) {
				continue
			}
			if strings.EqualFold(pvc.Status.Phase, "Pending") {
				events = append(events, types.DiagnosticEvent{
					ID:             fmt.Sprintf("pvc-pending:%s/%s", pvc.Metadata.Namespace, pvc.Metadata.Name),
					ClusterVersion: clusterVer,
					Namespace:      pvc.Metadata.Namespace,
					PVCName:        pvc.Metadata.Name,
					CurrentStorage: pvc.Spec.Resources.Requests["storage"],
					Reason:         "ProvisioningFailed",
					Message:        fmt.Sprintf("PersistentVolumeClaim %s/%s is stuck in Pending phase", pvc.Metadata.Namespace, pvc.Metadata.Name),
					Timestamp:      time.Now().UTC(),
				})
			}
		}
	}

	// 4. Scan Operator Custom Resources (e.g. CloudNativePG Cluster.postgresql.cnpg.io, NooBaa/ODF CRs)
	events = append(events, c.scanOperatorCustomResources(ctx, clusterVer)...)

	return events, nil
}

func (c *Client) scanOperatorCustomResources(ctx context.Context, clusterVer string) []types.DiagnosticEvent {
	var events []types.DiagnosticEvent

	// 4a. CloudNativePG Clusters (postgresql.cnpg.io/v1, Kind=Cluster)
	var cnpgList GenericK8sList
	if err := c.GetJSON(ctx, "/apis/postgresql.cnpg.io/v1/clusters", &cnpgList); err == nil {
		for _, item := range cnpgList.Items {
			ns := item.Metadata.Namespace
			name := item.Metadata.Name
			phase := extractStatusPhase(item.Status)
			conds := extractStatusConditions(item.Status)
			phaseLower := strings.ToLower(phase)

			memLim, _ := nestedString(item.Spec, "resources", "limits", "memory")
			memReq, _ := nestedString(item.Spec, "resources", "requests", "memory")
			storageSize, _ := nestedString(item.Spec, "storage", "size")

			// Check if phase indicates an unhealthy state (and hasn't already been remediated to >=256Mi when OOM)
			isUnhealthy := strings.Contains(phaseLower, "fail") ||
				strings.Contains(phaseLower, "degrad") ||
				strings.Contains(phaseLower, "oom") ||
				strings.Contains(phaseLower, "not enough disk") ||
				strings.Contains(phaseLower, "unhealthy")

			var reason, msg string
			exitCode := 0
			for _, cond := range conds {
				cl := strings.ToLower(cond)
				if strings.Contains(cl, "oom") || strings.Contains(cl, "137") {
					isUnhealthy = true
					reason = "OOMKilled"
					exitCode = 137
					msg = cond
				} else if strings.Contains(cl, "ready=false") || strings.Contains(cl, "degraded=true") {
					isUnhealthy = true
					if reason == "" {
						reason = "OperatorCRDegraded"
					}
					msg = cond
				}
			}

			if isUnhealthy {
				if reason == "" {
					if strings.Contains(phaseLower, "oom") {
						reason = "OOMKilled"
						exitCode = 137
					} else if strings.Contains(phaseLower, "disk") {
						reason = "PVCFilesystemFull"
					} else {
						reason = "OperatorCRDegraded"
					}
				}
				if msg == "" {
					msg = fmt.Sprintf("CloudNativePG Cluster.postgresql.cnpg.io %s/%s is in phase %q (memoryLimit=%s, storage=%s)", ns, name, phase, memLim, storageSize)
				}
				tree, _ := c.ResolveOwnershipHierarchy(ctx, ns, "postgresql.cnpg.io/v1", "Cluster", name)
				var chain []types.OwnerChainLink
				if tree != nil {
					chain = tree.OwnershipChain
				}
				ev := types.DiagnosticEvent{
					ID:               fmt.Sprintf("cnpg:%s/%s:%s:%s", ns, name, reason, memLim),
					ClusterVersion:   clusterVer,
					Namespace:        ns,
					OwnerAPIVersion:  "postgresql.cnpg.io/v1",
					OwnerKind:        "Cluster",
					OwnerName:        name,
					OperatorAdapter:  "cloudnative-pg",
					OwnershipChain:   chain,
					ExitCode:         exitCode,
					Reason:           reason,
					Message:          msg,
					CurrentMemoryLim: memLim,
					CurrentMemoryReq: memReq,
					CurrentStorage:   storageSize,
					Labels:           item.Metadata.Labels,
					Timestamp:        time.Now().UTC(),
				}
				c.populateQuota(ctx, ns, &ev)
				events = append(events, ev)
			}
		}
	}

	// 4b. NooBaa / ODF Custom Resources (noobaa.io/v1alpha1, Kind=NooBaa & BackingStore)
	var noobaaList GenericK8sList
	if err := c.GetJSON(ctx, "/apis/noobaa.io/v1alpha1/noobaas", &noobaaList); err == nil {
		for _, item := range noobaaList.Items {
			ns := item.Metadata.Namespace
			name := item.Metadata.Name
			phase := extractStatusPhase(item.Status)
			if strings.EqualFold(phase, "Rejected") || strings.EqualFold(phase, "Degraded") || strings.EqualFold(phase, "Failing") {
				tree, _ := c.ResolveOwnershipHierarchy(ctx, ns, "noobaa.io/v1alpha1", "NooBaa", name)
				var chain []types.OwnerChainLink
				if tree != nil {
					chain = append(tree.OwnershipChain, tree.RelatedCRs...)
				}
				ev := types.DiagnosticEvent{
					ID:              fmt.Sprintf("noobaa:%s/%s:%s", ns, name, phase),
					ClusterVersion:  clusterVer,
					Namespace:       ns,
					OwnerAPIVersion: "noobaa.io/v1alpha1",
					OwnerKind:       "NooBaa",
					OwnerName:       name,
					OperatorAdapter: "noobaa-odf",
					OwnershipChain:  chain,
					Reason:          "OperatorCRDegraded",
					Message:         fmt.Sprintf("NooBaa.noobaa.io %s/%s is in phase %q across multi-CR hierarchy", ns, name, phase),
					Labels:          item.Metadata.Labels,
					Timestamp:       time.Now().UTC(),
				}
				c.populateQuota(ctx, ns, &ev)
				events = append(events, ev)
			}
		}
	}

	return events
}

// EnrichDiagnosticEvent queries the live Kubernetes API server to fill in missing Pod,
// Owner (StatefulSet/Deployment), Container memory limits, and Namespace ResourceQuota fields.
func (c *Client) EnrichDiagnosticEvent(ctx context.Context, ev *types.DiagnosticEvent) {
	if c.simulate || ev == nil {
		return
	}
	if ev.Namespace == "" {
		ev.Namespace = "default"
	}
	if ev.ClusterVersion == "" {
		ev.ClusterVersion = c.detectClusterVersion(ctx)
	}

	if ev.PodName != "" {
		var pod k8sPodItem
		podPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", ev.Namespace, ev.PodName)
		if err := c.GetJSON(ctx, podPath, &pod); err == nil {
			if ev.OwnerName == "" || ev.OwnerKind == "" {
				ev.OwnerKind, ev.OwnerName = c.resolveOwner(ctx, ev.Namespace, pod.Metadata.OwnerReferences, pod.Metadata.Name)
			}
			if ev.ContainerName == "" && len(pod.Spec.Containers) > 0 {
				ev.ContainerName = pod.Spec.Containers[0].Name
			}
			lim, req := extractContainerMemory(pod, ev.ContainerName)
			if ev.CurrentMemoryLim == "" {
				ev.CurrentMemoryLim = lim
			}
			if ev.CurrentMemoryReq == "" {
				ev.CurrentMemoryReq = req
			}
			for _, cs := range pod.Status.ContainerStatuses {
				if (cs.State.Terminated != nil && cs.State.Terminated.Reason == "OOMKilled") ||
					(cs.LastState.Terminated != nil && cs.LastState.Terminated.Reason == "OOMKilled") {
					ev.ExitCode = 137
					if ev.Reason == "" {
						ev.Reason = "OOMKilled"
					}
				}
			}
		}
	}

	if ev.OwnerName != "" && (ev.CurrentMemoryLim == "" || ev.ContainerName == "") {
		lim, req := c.getWorkloadMemory(ctx, ev.Namespace, ev.OwnerKind, ev.OwnerName, ev.ContainerName)
		if ev.CurrentMemoryLim == "" && lim != "" {
			ev.CurrentMemoryLim = lim
		}
		if ev.CurrentMemoryReq == "" && req != "" {
			ev.CurrentMemoryReq = req
		}
	}

	c.populateOwnershipChainAndQuota(ctx, ev.Namespace, ev)
}

func (c *Client) populateOwnershipChainAndQuota(ctx context.Context, namespace string, ev *types.DiagnosticEvent) {
	if ev == nil {
		return
	}
	if len(ev.OwnershipChain) == 0 {
		seedKind := "Pod"
		seedName := ev.PodName
		seedAPIVersion := "v1"
		if seedName == "" && ev.OwnerName != "" {
			seedKind = ev.OwnerKind
			seedName = ev.OwnerName
			seedAPIVersion = ev.OwnerAPIVersion
		}
		if seedName != "" {
			if tree, err := c.ResolveOwnershipHierarchy(ctx, namespace, seedAPIVersion, seedKind, seedName); err == nil && tree != nil && len(tree.OwnershipChain) > 0 {
				ev.OwnershipChain = tree.OwnershipChain
				if tree.OperatorAdapter != "" {
					ev.OperatorAdapter = tree.OperatorAdapter
				}
				if tree.RootController != nil && tree.RootController.Name != "" && !strings.EqualFold(tree.RootController.Kind, "Pod") {
					// If an operator CR (e.g. Cluster.postgresql.cnpg.io or NooBaa.noobaa.io) owns this workload,
					// promote OwnerKind/OwnerName/OwnerAPIVersion to the controlling CR so remediation patches the CR rather than an ephemeral pod.
					if tree.OperatorAdapter != "" {
						ev.OwnerAPIVersion = tree.RootController.APIVersion
						ev.OwnerKind = tree.RootController.Kind
						ev.OwnerName = tree.RootController.Name
						if memLim := tree.RootController.SpecFields["memoryLimit"]; memLim != "" {
							ev.CurrentMemoryLim = memLim
						}
						if memReq := tree.RootController.SpecFields["memoryRequest"]; memReq != "" {
							ev.CurrentMemoryReq = memReq
						}
					} else if ev.OwnerName == "" {
						ev.OwnerAPIVersion = tree.RootController.APIVersion
						ev.OwnerKind = tree.RootController.Kind
						ev.OwnerName = tree.RootController.Name
					}
				}
			}
		}
	}
	c.populateQuota(ctx, namespace, ev)
}

func (c *Client) detectClusterVersion(ctx context.Context) string {
	var ver struct {
		GitVersion string `json:"gitVersion"`
	}
	if err := c.GetJSON(ctx, "/version", &ver); err == nil && ver.GitVersion != "" {
		return ver.GitVersion
	}
	return "v1.35.2"
}

func (c *Client) resolveOwner(ctx context.Context, namespace string, refs []k8sOwnerRef, podName string) (string, string) {
	for _, ref := range refs {
		switch ref.Kind {
		case "StatefulSet":
			return "StatefulSet", ref.Name
		case "ReplicaSet":
			// Resolve parent Deployment from ReplicaSet
			var rs struct {
				Metadata struct {
					OwnerReferences []k8sOwnerRef `json:"ownerReferences"`
				} `json:"metadata"`
			}
			rsPath := fmt.Sprintf("/apis/apps/v1/namespaces/%s/replicasets/%s", namespace, ref.Name)
			if err := c.GetJSON(ctx, rsPath, &rs); err == nil {
				for _, pRef := range rs.Metadata.OwnerReferences {
					if pRef.Kind == "Deployment" {
						return "Deployment", pRef.Name
					}
				}
			}
			if idx := strings.LastIndex(ref.Name, "-"); idx > 0 {
				return "Deployment", ref.Name[:idx]
			}
			return "Deployment", ref.Name
		case "DaemonSet":
			return "DaemonSet", ref.Name
		default:
			if ref.Kind != "" && ref.Name != "" {
				return ref.Kind, ref.Name
			}
		}
	}
	if idx := strings.LastIndex(podName, "-"); idx > 0 {
		return "StatefulSet", podName[:idx]
	}
	return "StatefulSet", podName
}

func (c *Client) getWorkloadMemory(ctx context.Context, namespace, ownerKind, ownerName, containerName string) (string, string) {
	if ownerName == "" {
		return "", ""
	}
	plural := "statefulsets"
	if strings.EqualFold(ownerKind, "Deployment") {
		plural = "deployments"
	}
	var wl struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Name      string `json:"name"`
						Resources struct {
							Limits   map[string]string `json:"limits"`
							Requests map[string]string `json:"requests"`
						} `json:"resources"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s", namespace, plural, ownerName)
	if err := c.GetJSON(ctx, path, &wl); err == nil && len(wl.Spec.Template.Spec.Containers) > 0 {
		for _, ctr := range wl.Spec.Template.Spec.Containers {
			if containerName == "" || ctr.Name == containerName {
				return ctr.Resources.Limits["memory"], ctr.Resources.Requests["memory"]
			}
		}
		ctr := wl.Spec.Template.Spec.Containers[0]
		return ctr.Resources.Limits["memory"], ctr.Resources.Requests["memory"]
	}
	return "", ""
}

func (c *Client) populateQuota(ctx context.Context, namespace string, ev *types.DiagnosticEvent) {
	if ev.QuotaMemoryLimit != "" && ev.QuotaName != "" {
		return
	}
	var qList k8sResourceQuotaList
	path := fmt.Sprintf("/api/v1/namespaces/%s/resourcequotas", namespace)
	if err := c.GetJSON(ctx, path, &qList); err == nil && len(qList.Items) > 0 {
		q := qList.Items[0]
		ev.QuotaName = q.Metadata.Name
		if lim := q.Status.Hard["limits.memory"]; lim != "" {
			ev.QuotaMemoryLimit = lim
		} else if lim := q.Status.Hard["memory"]; lim != "" {
			ev.QuotaMemoryLimit = lim
		}
		if used := q.Status.Used["limits.memory"]; used != "" {
			ev.QuotaMemoryUsed = used
		} else if used := q.Status.Used["memory"]; used != "" {
			ev.QuotaMemoryUsed = used
		}
	}
}

func extractContainerMemory(pod k8sPodItem, containerName string) (limit string, request string) {
	for _, ctr := range pod.Spec.Containers {
		if containerName == "" || ctr.Name == containerName {
			return ctr.Resources.Limits["memory"], ctr.Resources.Requests["memory"]
		}
	}
	if len(pod.Spec.Containers) > 0 {
		ctr := pod.Spec.Containers[0]
		return ctr.Resources.Limits["memory"], ctr.Resources.Requests["memory"]
	}
	return "", ""
}

func isSystemNamespace(ns string) bool {
	switch ns {
	case "kube-system", "kube-public", "kube-node-lease", "local-path-storage", "kube-doctor-system":
		return true
	default:
		return false
	}
}

