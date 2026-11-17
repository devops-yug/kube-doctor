package rag

import (
	"encoding/json"
	"os"
	"sort"
	"strings"
	"sync"
)

// Document represents a knowledge chunk for Kubernetes >= 1.35 and OpenShift >= 4.22.
type Document struct {
	ID       string   `json:"id"`
	Version  string   `json:"version"`
	Category string   `json:"category"`
	Title    string   `json:"title"`
	Tags     []string `json:"tags"`
	Content  string   `json:"content"`
}

// Engine provides retrieval-augmented generation over K8s 1.35+ and OpenShift 4.22+ runbooks.
type Engine struct {
	mu   sync.RWMutex
	docs []Document
}

// NewEngine initializes the RAG engine from a JSON file path or falls back to embedded defaults.
func NewEngine(knowledgePath string) *Engine {
	e := &Engine{
		docs: defaultDocuments(),
	}
	if knowledgePath != "" {
		if data, err := os.ReadFile(knowledgePath); err == nil {
			var loaded []Document
			if err := json.Unmarshal(data, &loaded); err == nil && len(loaded) > 0 {
				e.docs = loaded
			}
		}
	}
	return e
}

// Retrieve returns the top-K relevant knowledge documents for a given query or diagnostic context.
func (e *Engine) Retrieve(query string, topK int) []Document {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if topK <= 0 {
		topK = 3
	}

	terms := tokenize(query)
	type scoredDoc struct {
		doc   Document
		score float64
	}

	var scored []scoredDoc
	for _, d := range e.docs {
		score := scoreDocument(d, terms)
		if score > 0 {
			scored = append(scored, scoredDoc{doc: d, score: score})
		}
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	if len(scored) == 0 {
		// Return top default runbooks if no specific term matched
		limit := topK
		if limit > len(e.docs) {
			limit = len(e.docs)
		}
		return e.docs[:limit]
	}

	var result []Document
	for i := 0; i < len(scored) && i < topK; i++ {
		result = append(result, scored[i].doc)
	}
	return result
}

func tokenize(text string) []string {
	cleaned := strings.ToLower(text)
	replacer := strings.NewReplacer(
		",", " ", ".", " ", ":", " ", ";", " ",
		"(", " ", ")", " ", "[", " ", "]", " ",
		"/", " ", "-", " ", "_", " ", "\n", " ",
	)
	cleaned = replacer.Replace(cleaned)
	parts := strings.Fields(cleaned)
	var out []string
	for _, p := range parts {
		if len(p) >= 2 {
			out = append(out, p)
		}
	}
	return out
}

func scoreDocument(d Document, queryTerms []string) float64 {
	var score float64
	titleLower := strings.ToLower(d.Title)
	contentLower := strings.ToLower(d.Content)
	catLower := strings.ToLower(d.Category)

	for _, term := range queryTerms {
		for _, tag := range d.Tags {
			if strings.ToLower(tag) == term || strings.Contains(strings.ToLower(tag), term) {
				score += 4.0
			}
		}
		if strings.Contains(titleLower, term) {
			score += 3.0
		}
		if strings.Contains(catLower, term) {
			score += 1.5
		}
		if strings.Contains(contentLower, term) {
			score += 1.0
		}
	}
	return score
}

func defaultDocuments() []Document {
	return []Document{
		{
			ID:       "k8s-1.35-inplace-pod-vertical-scaling",
			Version:  "Kubernetes >= 1.35 / OpenShift >= 4.22",
			Category: "Resource Management & OOM Remediation",
			Title:    "In-Place Pod Vertical Scaling (InPlacePodVerticalScaling GA in 1.35+)",
			Tags:     []string{"oomkilled", "137", "memory", "resize", "statefulset", "pod", "resources", "1.35"},
			Content:  "In Kubernetes 1.35+ (and OpenShift 4.22+), InPlacePodVerticalScaling is GA and uses the pods/resize subresource. When a StatefulSet pod is OOMKilled (ExitCode 137), kube-doctor checks the Namespace ResourceQuota first. If within quota, it updates the StatefulSet container memory requests/limits and patches pods/resize to recover the pod without losing stateful identity.",
		},
		{
			ID:       "k8s-1.35-statefulset-oom-quota-governance",
			Version:  "Kubernetes >= 1.35 / OpenShift >= 4.22",
			Category: "StatefulSet & Namespace ResourceQuota Governance",
			Title:    "Handling StatefulSet OOMKilled When Namespace ResourceQuota Is Exceeded",
			Tags:     []string{"statefulset", "oomkilled", "resourcequota", "quota", "exceeded", "approval", "gitops"},
			Content:  "When a StatefulSet pod is OOMKilled and bumping its memory limit would exceed the Namespace ResourceQuota (or quota is already exceeded), kube-doctor requires human approval (Policy=APPROVAL_REQUIRED). It creates a RemediationRequest CRD and opens a GitOps Pull Request updating both the Namespace ResourceQuota and the StatefulSet memory resources.",
		},
		{
			ID:       "ocp-4.22-machineset-autoscaling-remediation",
			Version:  "OpenShift >= 4.22 (Kubernetes 1.35+)",
			Category: "Cluster Infrastructure & Machine API / CAPI",
			Title:    "OpenShift 4.22+ Missing Machine & MachineSet Autoscaling with Approval",
			Tags:     []string{"machineset", "machine", "openshift", "4.22", "autoscale", "node", "missing", "unschedulable", "approval"},
			Content:  "In OpenShift 4.22+, worker capacity is managed via MachineSet (machine.openshift.io/v1beta1) and Cluster API (cluster.x-k8s.io/v1beta1). If a Machine is missing or in Failed state, kube-doctor classifies the issue as P1-HIGH / SEV-2 with Policy=APPROVAL_REQUIRED, generating a RemediationRequest CRD and GitOps PR to scale MachineSet replicas upon approval.",
		},
	}
}

