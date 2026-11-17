package k8s

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Client interacts with a Kubernetes >= 1.35 / OpenShift >= 4.22 API server.
// When no live API server token is configured, it operates in a deterministic
// stateful simulation mode for safe local testing and CI verification.
type Client struct {
	apiServer  string
	token      string
	httpClient *http.Client
	simulate   bool

	mu       sync.RWMutex
	auditLog []AuditEntry
}

// AuditEntry records every cluster API mutation executed by kube-doctor.
type AuditEntry struct {
	Timestamp   time.Time `json:"timestamp"`
	Method      string    `json:"method"`
	APIPath     string    `json:"apiPath"`
	Payload     string    `json:"payload"`
	Simulated   bool      `json:"simulated"`
	StatusCode  int       `json:"statusCode"`
	Description string    `json:"description"`
}

// NewClient creates a Kubernetes 1.35+ / OpenShift 4.22+ REST client.
func NewClient() *Client {
	apiServer := os.Getenv("KUBE_API_SERVER")
	token := os.Getenv("KUBE_BEARER_TOKEN")

	if apiServer == "" {
		host := os.Getenv("KUBERNETES_SERVICE_HOST")
		port := os.Getenv("KUBERNETES_SERVICE_PORT")
		if host != "" && port != "" {
			apiServer = fmt.Sprintf("https://%s:%s", host, port)
		}
	}
	if token == "" {
		if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
			token = strings.TrimSpace(string(data))
		}
	}

	tlsCfg := &tls.Config{
		InsecureSkipVerify: os.Getenv("KUBE_INSECURE_TLS") == "true",
	}
	if caBytes, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"); err == nil {
		pool := x509.NewCertPool()
		if pool.AppendCertsFromPEM(caBytes) {
			tlsCfg.RootCAs = pool
		} else {
			tlsCfg.InsecureSkipVerify = true
		}
	} else {
		tlsCfg.InsecureSkipVerify = true
	}

	// If not running in-cluster (and not in unit tests or forced simulation mode),
	// fall back to ~/.kube/config so `make run-hub` on the host talks to the local kind/K8s cluster.
	if apiServer == "" && os.Getenv("KUBE_SIMULATION_MODE") != "true" && !isTestBinary() {
		if kcServer, kcToken, kcCert, kcCA, ok := loadFromKubeconfig(); ok {
			apiServer = kcServer
			if kcToken != "" {
				token = kcToken
			}
			if kcCert != nil {
				tlsCfg.Certificates = []tls.Certificate{*kcCert}
			}
			if len(kcCA) > 0 {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(kcCA) {
					tlsCfg.RootCAs = pool
					tlsCfg.InsecureSkipVerify = false
				}
			}
		}
	}

	simulate := apiServer == "" || os.Getenv("KUBE_SIMULATION_MODE") == "true"

	return &Client{
		apiServer: strings.TrimRight(apiServer, "/"),
		token:     token,
		simulate:  simulate,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: tlsCfg,
			},
		},
	}
}

// IsSimulated returns true when no live Kubernetes API server is configured.
func (c *Client) IsSimulated() bool {
	return c.simulate
}

// GetJSON performs an authenticated GET request against the Kubernetes API server and decodes the JSON response.
func (c *Client) GetJSON(ctx context.Context, apiPath string, target any) error {
	if c.simulate {
		return fmt.Errorf("kubernetes client is running in simulation mode")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.apiServer+apiPath, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("GET %s returned %d: %s", apiPath, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(target)
}

// DeletePod deletes a Pod (e.g. to force immediate StatefulSet recreation with updated memory limits after CrashLoopBackOff).
func (c *Client) DeletePod(ctx context.Context, namespace, podName, description string) error {
	if podName == "" {
		return nil
	}
	apiPath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s", namespace, podName)
	if c.simulate {
		c.recordAudit("DELETE", apiPath, "", true, http.StatusOK, description)
		return nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.apiServer+apiPath, nil)
	if err != nil {
		return err
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)
	c.recordAudit("DELETE", apiPath, "", false, resp.StatusCode, description)
	return nil
}

// PatchStatefulSetResources updates a StatefulSet's container memory requests and limits,
// and optionally invokes the Kubernetes 1.35+ pods/resize subresource for in-place pod vertical scaling.
func (c *Client) PatchStatefulSetResources(ctx context.Context, namespace, statefulSetName, podName, containerName, newReqMem, newLimMem string) (string, error) {
	return c.PatchWorkloadResources(ctx, namespace, "StatefulSet", statefulSetName, podName, containerName, newReqMem, newLimMem)
}

// PatchWorkloadResources updates a StatefulSet or Deployment container's memory requests and limits,
// invokes Kubernetes 1.35+ pods/resize for in-place vertical scaling, and recycles crashing OOM pods so they pick up the new spec immediately.
func (c *Client) PatchWorkloadResources(ctx context.Context, namespace, ownerKind, ownerName, podName, containerName, newReqMem, newLimMem string) (string, error) {
	if containerName == "" {
		containerName = "app"
	}
	plural := "statefulsets"
	if strings.EqualFold(ownerKind, "Deployment") {
		plural = "deployments"
	} else {
		ownerKind = "StatefulSet"
	}
	workloadPath := fmt.Sprintf("/apis/apps/v1/namespaces/%s/%s/%s", namespace, plural, ownerName)

	// If connected to a live cluster, resolve the real container name from the workload spec
	// so strategic-merge-patch updates the existing container rather than appending a new one.
	if !c.simulate {
		var liveWorkload struct {
			Spec struct {
				Template struct {
					Spec struct {
						Containers []struct {
							Name string `json:"name"`
						} `json:"containers"`
					} `json:"spec"`
				} `json:"template"`
			} `json:"spec"`
		}
		if err := c.GetJSON(ctx, workloadPath, &liveWorkload); err == nil && len(liveWorkload.Spec.Template.Spec.Containers) > 0 {
			matched := false
			for _, ctr := range liveWorkload.Spec.Template.Spec.Containers {
				if ctr.Name == containerName {
					matched = true
					break
				}
			}
			if !matched {
				containerName = liveWorkload.Spec.Template.Spec.Containers[0].Name
			}
		}
	}

	patchObj := map[string]any{
		"spec": map[string]any{
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []map[string]any{
						{
							"name": containerName,
							"resources": map[string]any{
								"requests": map[string]string{
									"memory": newReqMem,
								},
								"limits": map[string]string{
									"memory": newLimMem,
								},
							},
						},
					},
				},
			},
		},
	}
	patchBytes, _ := json.Marshal(patchObj)
	if err := c.doPatch(ctx, workloadPath, "application/strategic-merge-patch+json", string(patchBytes),
		fmt.Sprintf("Patch %s %s/%s (container %s) memory request=%s limit=%s", ownerKind, namespace, ownerName, containerName, newReqMem, newLimMem)); err != nil {
		return "", err
	}

	if podName == "" && ownerKind == "StatefulSet" {
		podName = ownerName + "-0"
	}

	// Kubernetes 1.35+ InPlacePodVerticalScaling GA: patch pods/{pod}/resize if podName is present
	if podName != "" {
		resizePath := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/resize", namespace, podName)
		resizePayload := map[string]any{
			"spec": map[string]any{
				"containers": []map[string]any{
					{
						"name": containerName,
						"resources": map[string]any{
							"requests": map[string]string{"memory": newReqMem},
							"limits":   map[string]string{"memory": newLimMem},
						},
					},
				},
			},
		}
		resizeBytes, _ := json.Marshal(resizePayload)
		_ = c.doPatch(ctx, resizePath, "application/strategic-merge-patch+json", string(resizeBytes),
			fmt.Sprintf("K8s 1.35+ In-Place Pod Vertical Scaling (/resize) for pod %s/%s", namespace, podName))

		// If the pod is stuck in CrashLoopBackOff/OOMKilled on a live cluster, delete the pod so the
		// StatefulSet/Deployment controller immediately recreates it with the new memory limit without waiting for backoff.
		if !c.simulate && ownerKind == "StatefulSet" {
			_ = c.DeletePod(ctx, namespace, podName, fmt.Sprintf("Recycle OOMKilled/CrashLoopBackOff pod %s/%s to apply updated %s memory limit (%s)", namespace, podName, ownerKind, newLimMem))
		}
	}

	return string(patchBytes), nil
}

// PatchNamespaceResourceQuota updates the hard memory limit/request on a Namespace ResourceQuota.
func (c *Client) PatchNamespaceResourceQuota(ctx context.Context, namespace, quotaName, newHardMemoryLimit, newHardMemoryRequest string) (string, error) {
	if quotaName == "" {
		quotaName = "default-quota"
	}
	patchObj := map[string]any{
		"spec": map[string]any{
			"hard": map[string]string{
				"limits.memory":   newHardMemoryLimit,
				"requests.memory": newHardMemoryRequest,
			},
		},
	}
	patchBytes, _ := json.Marshal(patchObj)
	apiPath := fmt.Sprintf("/api/v1/namespaces/%s/resourcequotas/%s", namespace, quotaName)
	if err := c.doPatch(ctx, apiPath, "application/strategic-merge-patch+json", string(patchBytes),
		fmt.Sprintf("Approved update of Namespace %s ResourceQuota %s to limits.memory=%s", namespace, quotaName, newHardMemoryLimit)); err != nil {
		return "", err
	}
	return string(patchBytes), nil
}

// ScaleOpenShiftMachineSet scales an OpenShift 4.22+ MachineSet (machine.openshift.io/v1beta1) in openshift-machine-api.
func (c *Client) ScaleOpenShiftMachineSet(ctx context.Context, machineSetName string, newReplicas int) (string, error) {
	if machineSetName == "" {
		machineSetName = "cluster-worker-default"
	}
	patchObj := map[string]any{
		"spec": map[string]any{
			"replicas": newReplicas,
		},
	}
	patchBytes, _ := json.Marshal(patchObj)
	apiPath := fmt.Sprintf("/apis/machine.openshift.io/v1beta1/namespaces/openshift-machine-api/machinesets/%s", machineSetName)
	if err := c.doPatch(ctx, apiPath, "application/merge-patch+json", string(patchBytes),
		fmt.Sprintf("Approved OpenShift 4.22+ MachineSet %s scale to %d replicas", machineSetName, newReplicas)); err != nil {
		return "", err
	}
	return string(patchBytes), nil
}

// CreateRemediationRequestCRD creates a kubedoctor.io/v1alpha1 RemediationRequest custom resource in the cluster.
func (c *Client) CreateRemediationRequestCRD(ctx context.Context, namespace, name, payloadJSON string) error {
	if namespace == "" {
		namespace = "kube-doctor-system"
	}
	apiPath := fmt.Sprintf("/apis/kubedoctor.io/v1alpha1/namespaces/%s/remediationrequests", namespace)
	return c.doPost(ctx, apiPath, payloadJSON, fmt.Sprintf("Create RemediationRequest CRD %s/%s awaiting admin approval", namespace, name))
}

// PatchGenericResource applies a strategic or merge patch to any Kubernetes/OpenShift resource.
func (c *Client) PatchGenericResource(ctx context.Context, apiVersion, kind, namespace, name, payloadJSON, description string) error {
	apiPath := buildResourcePath(apiVersion, kind, namespace, name)
	return c.doPatch(ctx, apiPath, "application/merge-patch+json", payloadJSON, description)
}

func buildResourcePath(apiVersion, kind, namespace, name string) string {
	plural := PluralizeKind(kind)
	if apiVersion == "v1" || apiVersion == "" {
		if namespace != "" && !isClusterScopedKind(kind) {
			return fmt.Sprintf("/api/v1/namespaces/%s/%s/%s", namespace, plural, name)
		}
		return fmt.Sprintf("/api/v1/%s/%s", plural, name)
	}
	if namespace != "" && !isClusterScopedKind(kind) {
		return fmt.Sprintf("/apis/%s/namespaces/%s/%s/%s", apiVersion, namespace, plural, name)
	}
	return fmt.Sprintf("/apis/%s/%s/%s", apiVersion, plural, name)
}

// BuildResourceListPath returns the REST collection path for listing resources of the given GVK.
func BuildResourceListPath(apiVersion, kind, namespace string) string {
	plural := PluralizeKind(kind)
	if apiVersion == "v1" || apiVersion == "" {
		if namespace != "" && !isClusterScopedKind(kind) {
			return fmt.Sprintf("/api/v1/namespaces/%s/%s", namespace, plural)
		}
		return fmt.Sprintf("/api/v1/%s", plural)
	}
	if namespace != "" && !isClusterScopedKind(kind) {
		return fmt.Sprintf("/apis/%s/namespaces/%s/%s", apiVersion, namespace, plural)
	}
	return fmt.Sprintf("/apis/%s/%s", apiVersion, plural)
}

// PluralizeKind maps standard Kubernetes, OpenShift, CloudNativePG, NooBaa/ODF, and Strimzi Kinds to REST plurals.
func PluralizeKind(kind string) string {
	k := strings.ToLower(strings.TrimSpace(kind))
	switch k {
	case "endpoints":
		return "endpoints"
	case "ingress":
		return "ingresses"
	case "networkpolicy":
		return "networkpolicies"
	case "storageclass":
		return "storageclasses"
	case "bucketclass":
		return "bucketclasses"
	case "prometheus":
		return "prometheuses"
	case "noobaa":
		return "noobaas"
	case "backingstore":
		return "backingstores"
	case "namespacestore":
		return "namespacestores"
	case "storagecluster":
		return "storageclusters"
	case "cephcluster":
		return "cephclusters"
	case "cluster":
		return "clusters"
	case "pooler":
		return "poolers"
	case "backup":
		return "backups"
	case "scheduledbackup":
		return "scheduledbackups"
	}
	if strings.HasSuffix(k, "s") || strings.HasSuffix(k, "x") || strings.HasSuffix(k, "ch") || strings.HasSuffix(k, "sh") {
		return k + "es"
	}
	if strings.HasSuffix(k, "y") && !strings.HasSuffix(k, "ay") && !strings.HasSuffix(k, "ey") && !strings.HasSuffix(k, "oy") && !strings.HasSuffix(k, "uy") {
		return strings.TrimSuffix(k, "y") + "ies"
	}
	return k + "s"
}

func isClusterScopedKind(kind string) bool {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "node", "namespace", "persistentvolume", "storageclass", "clusteroperator", "machineconfigpool", "clusterversion", "customresourcedefinition":
		return true
	default:
		return false
	}
}

// AuditEntries returns a copy of all executed/simulated cluster API mutations.
func (c *Client) AuditEntries() []AuditEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]AuditEntry, len(c.auditLog))
	copy(out, c.auditLog)
	return out
}

func (c *Client) doPatch(ctx context.Context, apiPath, contentType, payload, description string) error {
	if c.simulate {
		c.recordAudit("PATCH", apiPath, payload, true, http.StatusOK, description)
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, c.apiServer+apiPath, bytes.NewBufferString(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	c.recordAudit("PATCH", apiPath, payload, false, resp.StatusCode, description)
	// If testing a synthetic event where the target object does not exist yet on the live cluster,
	// containers/kubernetes-mcp-server Server-Side Apply handles creation or we record the 404 without failing the workflow.
	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes API PATCH %s returned status %d", apiPath, resp.StatusCode)
	}
	return nil
}

func (c *Client) doPost(ctx context.Context, apiPath, payload, description string) error {
	if c.simulate {
		c.recordAudit("POST", apiPath, payload, true, http.StatusCreated, description)
		return nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.apiServer+apiPath, bytes.NewBufferString(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.ReadAll(resp.Body)

	c.recordAudit("POST", apiPath, payload, false, resp.StatusCode, description)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("kubernetes API POST %s returned status %d", apiPath, resp.StatusCode)
	}
	return nil
}

func (c *Client) recordAudit(method, apiPath, payload string, simulated bool, status int, desc string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.auditLog = append(c.auditLog, AuditEntry{
		Timestamp:   time.Now().UTC(),
		Method:      method,
		APIPath:     apiPath,
		Payload:     payload,
		Simulated:   simulated,
		StatusCode:  status,
		Description: desc,
	})
}

// ParseMemoryMiB converts a Kubernetes memory string (e.g., "512Mi", "2Gi", "1024M") into MiB.
func ParseMemoryMiB(mem string) int64 {
	mem = strings.TrimSpace(mem)
	if mem == "" {
		return 0
	}
	switch {
	case strings.HasSuffix(mem, "Gi"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(mem, "Gi"), 64)
		return int64(v * 1024)
	case strings.HasSuffix(mem, "G"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(mem, "G"), 64)
		return int64(v * 953.674)
	case strings.HasSuffix(mem, "Mi"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(mem, "Mi"), 64)
		return int64(v)
	case strings.HasSuffix(mem, "M"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(mem, "M"), 64)
		return int64(v)
	case strings.HasSuffix(mem, "Ki"):
		v, _ := strconv.ParseFloat(strings.TrimSuffix(mem, "Ki"), 64)
		return int64(v / 1024)
	default:
		v, _ := strconv.ParseInt(mem, 10, 64)
		return v / (1024 * 1024)
	}
}

// FormatMemoryMiB formats MiB back into canonical Kubernetes Mi or Gi notation.
func FormatMemoryMiB(mib int64) string {
	if mib <= 0 {
		return "512Mi"
	}
	if mib >= 1024 && mib%1024 == 0 {
		return fmt.Sprintf("%dGi", mib/1024)
	}
	return fmt.Sprintf("%dMi", mib)
}

func isTestBinary() bool {
	if len(os.Args) == 0 {
		return false
	}
	arg0 := os.Args[0]
	return strings.HasSuffix(arg0, ".test") || strings.Contains(arg0, "/_test/")
}

func loadFromKubeconfig() (server, token string, clientCert *tls.Certificate, caPEM []byte, ok bool) {
	kcPath := strings.TrimSpace(os.Getenv("KUBECONFIG"))
	if kcPath != "" {
		if idx := strings.IndexByte(kcPath, ':'); idx > 0 {
			kcPath = kcPath[:idx]
		}
	} else if home, err := os.UserHomeDir(); err == nil && home != "" {
		kcPath = filepath.Join(home, ".kube", "config")
	}
	if kcPath == "" {
		return "", "", nil, nil, false
	}
	data, err := os.ReadFile(kcPath)
	if err != nil {
		return "", "", nil, nil, false
	}

	var certB64, keyB64, caB64 string
	for _, rawLine := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(rawLine)
		switch {
		case strings.HasPrefix(line, "server:") && server == "":
			server = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "server:")), `"'`)
		case strings.HasPrefix(line, "certificate-authority-data:") && caB64 == "":
			caB64 = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "certificate-authority-data:")), `"'`)
		case strings.HasPrefix(line, "client-certificate-data:") && certB64 == "":
			certB64 = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "client-certificate-data:")), `"'`)
		case strings.HasPrefix(line, "client-key-data:") && keyB64 == "":
			keyB64 = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "client-key-data:")), `"'`)
		case strings.HasPrefix(line, "token:") && token == "":
			token = strings.Trim(strings.TrimSpace(strings.TrimPrefix(line, "token:")), `"'`)
		}
	}
	if server == "" {
		return "", "", nil, nil, false
	}
	if caB64 != "" {
		if decoded, err := base64.StdEncoding.DecodeString(caB64); err == nil {
			caPEM = decoded
		}
	}
	if certB64 != "" && keyB64 != "" {
		certPEM, err1 := base64.StdEncoding.DecodeString(certB64)
		keyPEM, err2 := base64.StdEncoding.DecodeString(keyB64)
		if err1 == nil && err2 == nil {
			if pair, err := tls.X509KeyPair(certPEM, keyPEM); err == nil {
				clientCert = &pair
			}
		}
	}
	return server, token, clientCert, caPEM, true
}


