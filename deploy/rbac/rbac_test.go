// Package rbac_test is a fast, always-on guard over the shipped read-only RBAC
// floor (deploy/rbac/clusterrole.yaml). It runs in the no-Docker per-save loop
// (no build tag) and complements the heavier e2e proof in test/e2e/rbac_test.go:
// where the e2e test proves the floor HOLDS against a real apiserver, this test
// proves the YAML we ship is STILL read-only — it fails the build the moment
// anyone adds a write verb to the ClusterRole by accident.
package rbac_test

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"sigs.k8s.io/yaml"
)

// readOnlyVerbs is the complete set the floor is allowed to grant. Anything
// outside this set is a write (or escalation) verb and must fail the test.
var readOnlyVerbs = map[string]bool{
	"get":   true,
	"list":  true,
	"watch": true,
}

// clusterRolePath resolves clusterrole.yaml relative to this test file so the
// test is robust to the CWD the runner uses (go test runs with CWD = package
// dir, but deriving from the source location is unambiguous either way).
func clusterRolePath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed: cannot locate test source file")
	}
	return filepath.Join(filepath.Dir(thisFile), "clusterrole.yaml")
}

// loadClusterRole parses the shipped clusterrole.yaml into the typed object.
func loadClusterRole(t *testing.T) rbacv1.ClusterRole {
	t.Helper()
	data, err := os.ReadFile(clusterRolePath(t))
	if err != nil {
		t.Fatalf("read clusterrole.yaml: %v", err)
	}
	var cr rbacv1.ClusterRole
	if err := yaml.Unmarshal(data, &cr); err != nil {
		t.Fatalf("unmarshal clusterrole.yaml: %v", err)
	}
	return cr
}

// TestClusterRoleIsReadOnly is the load-bearing guard: every verb in every rule
// must be in the read-only allowlist. A wildcard `*` verb (or any write verb)
// fails here, catching an accidental escalation of the floor in the fast loop.
func TestClusterRoleIsReadOnly(t *testing.T) {
	cr := loadClusterRole(t)

	if cr.Kind != "ClusterRole" {
		t.Fatalf("kind = %q, want ClusterRole", cr.Kind)
	}
	if cr.Name != "ray-mcp-readonly" {
		t.Errorf("name = %q, want ray-mcp-readonly", cr.Name)
	}
	if len(cr.Rules) == 0 {
		t.Fatal("clusterrole has no rules")
	}

	for ruleIdx, rule := range cr.Rules {
		if len(rule.Verbs) == 0 {
			t.Errorf("rule %d (%v) has no verbs", ruleIdx, rule.Resources)
		}
		for _, verb := range rule.Verbs {
			if !readOnlyVerbs[verb] {
				t.Errorf("rule %d (apiGroups=%v resources=%v) grants non-read verb %q; the floor must be read-only (get/list/watch only)",
					ruleIdx, rule.APIGroups, rule.Resources, verb)
			}
		}
	}
}

// TestClusterRoleGrantsExpectedReads asserts the floor still covers exactly the
// reads the tools need: the KubeRay CRD read-triple and the core pods/events
// read. This catches the opposite mistake — silently NARROWING the floor below
// what the tools call, which would surface as runtime Forbidden errors.
func TestClusterRoleGrantsExpectedReads(t *testing.T) {
	cr := loadClusterRole(t)

	// allows reports whether some rule grants `verb` on (group, resource).
	allows := func(group, resource, verb string) bool {
		for _, rule := range cr.Rules {
			if contains(rule.APIGroups, group) && contains(rule.Resources, resource) && contains(rule.Verbs, verb) {
				return true
			}
		}
		return false
	}

	wantAllowed := []struct{ group, resource, verb string }{
		{"ray.io", "rayclusters", "get"},
		{"ray.io", "rayclusters", "list"},
		{"ray.io", "rayclusters", "watch"},
		{"ray.io", "rayjobs", "get"},
		{"ray.io", "rayservices", "get"},
		{"", "pods", "list"},
		{"", "events", "list"},
		{"", "pods", "get"},
		{"", "events", "get"},
	}
	for _, w := range wantAllowed {
		if !allows(w.group, w.resource, w.verb) {
			t.Errorf("floor does not grant %s on %q (group %q), but a tool needs it", w.verb, w.resource, w.group)
		}
	}
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
