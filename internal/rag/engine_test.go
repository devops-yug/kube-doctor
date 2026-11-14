package rag

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRAGEngineRetrieveAndLoad(t *testing.T) {
	// 1. Default documents fallback when path is empty or invalid
	eng := NewEngine("")
	docs := eng.Retrieve("OOMKilled StatefulSet pods/resize 1.35", 2)
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(docs))
	}
	if docs[0].ID != "k8s-1.35-inplace-pod-vertical-scaling" {
		t.Fatalf("unexpected top doc: %s", docs[0].ID)
	}

	// 2. Retrieve with topK <= 0 defaults to 3, and unmatched query returns default runbooks
	fallbackDocs := eng.Retrieve("completely-unmatched-xyz-999", 0)
	if len(fallbackDocs) != 3 {
		t.Fatalf("expected 3 fallback docs, got %d", len(fallbackDocs))
	}

	// 3. Retrieve with topK larger than doc count
	manyDocs := eng.Retrieve("unmatched-token-abc", 50)
	if len(manyDocs) != 3 {
		t.Fatalf("expected 3 docs when topK > len(docs), got %d", len(manyDocs))
	}

	// 4. Load custom JSON knowledge base file
	tmpDir := t.TempDir()
	kbFile := filepath.Join(tmpDir, "kb.json")
	customJSON := `[
		{
			"id": "custom-doc-1",
			"version": "1.35",
			"category": "Custom Storage",
			"title": "Cephfs Volume Expansion",
			"tags": ["cephfs", "pvc"],
			"content": "Expand cephfs volume online."
		}
	]`
	if err := os.WriteFile(kbFile, []byte(customJSON), 0600); err != nil {
		t.Fatal(err)
	}
	customEng := NewEngine(kbFile)
	matched := customEng.Retrieve("cephfs storage", 5)
	if len(matched) != 1 || matched[0].ID != "custom-doc-1" {
		t.Fatalf("expected custom-doc-1, got %+v", matched)
	}
}
