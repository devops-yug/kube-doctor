package types

import "testing"

func TestAllIssueTypes(t *testing.T) {
	issues := AllIssueTypes()
	if len(issues) != 27 {
		t.Fatalf("expected 27 issue types, got %d", len(issues))
	}
	seen := make(map[IssueType]bool)
	for _, it := range issues {
		if it == "" {
			t.Fatal("empty issue type")
		}
		if seen[it] {
			t.Fatalf("duplicate issue type: %s", it)
		}
		seen[it] = true
	}
}
