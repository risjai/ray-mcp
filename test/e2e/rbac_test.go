//go:build e2e

// Tier 5 (e2e) proof of the read-only RBAC floor (Task 7.5). This is what turns
// "we wrote a Role" into "the floor holds, both ways". It runs against the real
// kind apiserver (`make e2e-up`) and:
//
//  1. Applies the SHIPPED manifests in deploy/rbac/ via `kubectl apply -k`, so the
//     test validates the exact YAML we ship — not a hand-rebuilt copy.
//  2. Asks the apiserver, via SubjectAccessReview, what the ray-mcp ServiceAccount
//     is allowed to do, and asserts: the reads the tools make are ALLOWED, and
//     writes / secrets / pods-exec are DENIED.
//
// Why SubjectAccessReview rather than impersonation + real get/list calls?
// SAR is the apiserver's own authorization oracle: a single create against the
// authorization.k8s.io API answers "would user U be allowed to do verb V on
// resource R" without minting or using the SA's token and without performing any
// real mutating call to prove a write is denied (a real `create` would either be
// forbidden — fine — or actually create an object — a messy side effect). It is
// hermetic and tests exactly what RBAC grants. (Impersonation via client-go would
// also work and is a fine alternative; SAR is cleaner here because the negative
// assertions need no real writes.) The test runner needs permission to CREATE
// SubjectAccessReviews, which the kind cluster's default admin kubeconfig has.
//
// COMPILE-CHECKED in the no-Docker loop; RUN via `make e2e` on a machine with
// Docker + kind + the KubeRay operator installed.
package e2e

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	authzv1 "k8s.io/api/authorization/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// rayMCPNamespace / rayMCPSA mirror the ServiceAccount the shipped manifests
// create (deploy/rbac/serviceaccount.yaml, default namespace). The SAR username
// for a ServiceAccount is system:serviceaccount:<namespace>:<name>.
const (
	rayMCPNamespace = "default"
	rayMCPSA        = "ray-mcp"
	rayMCPUser      = "system:serviceaccount:" + rayMCPNamespace + ":" + rayMCPSA
)

// rbacManifestDir resolves deploy/rbac/ relative to this test file so
// `kubectl apply -k` targets the real shipped manifests regardless of CWD.
func rbacManifestDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed: cannot locate test source file")
	}
	// thisFile = <repo>/test/e2e/rbac_test.go ; manifests at <repo>/deploy/rbac.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(repoRoot, "deploy", "rbac")
}

// applyRBACManifests installs the shipped deploy/rbac/ bundle into the cluster via
// `kubectl apply -k`. Applying (rather than reconstructing the RBAC in Go) is what
// makes this test validate the YAML we actually ship. kubectl reads the same
// kubeconfig the controller-runtime client uses (the kind context).
func applyRBACManifests(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", "apply", "-k", rbacManifestDir(t))
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl apply -k deploy/rbac/ failed: %v\n%s", err, out.String())
	}
	t.Logf("applied deploy/rbac/:\n%s", out.String())
}

// allowed asks the apiserver whether the ray-mcp ServiceAccount may perform
// (verb, group, resource[, subresource]) and returns the authorization decision.
// It uses a SubjectAccessReview keyed on the SA's username, so it reflects exactly
// the RBAC the shipped ClusterRole + binding grant.
func allowed(ctx context.Context, t *testing.T, c client.Client, verb, group, resource, subresource string) bool {
	t.Helper()
	sar := &authzv1.SubjectAccessReview{
		Spec: authzv1.SubjectAccessReviewSpec{
			User: rayMCPUser,
			ResourceAttributes: &authzv1.ResourceAttributes{
				Namespace:   rayMCPNamespace,
				Verb:        verb,
				Group:       group,
				Resource:    resource,
				Subresource: subresource,
			},
		},
	}
	if err := c.Create(ctx, sar); err != nil {
		t.Fatalf("create SubjectAccessReview (verb=%s group=%q resource=%s sub=%q): %v", verb, group, resource, subresource, err)
	}
	return sar.Status.Allowed
}

// TestReadOnlyRBACFloor applies the shipped manifests and proves the floor holds
// both ways: the reads the tools make are allowed, and writes / sensitive verbs
// are denied for the ray-mcp ServiceAccount.
func TestReadOnlyRBACFloor(t *testing.T) {
	c := e2eClient(t)
	applyRBACManifests(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ALLOWED: every read the tools actually make, plus the proactively-granted
	// rayjobs/rayservices reads.
	allow := []struct {
		name                       string
		verb, group, resource, sub string
	}{
		{"get rayclusters", "get", "ray.io", "rayclusters", ""},
		{"list rayclusters", "list", "ray.io", "rayclusters", ""},
		{"watch rayclusters", "watch", "ray.io", "rayclusters", ""},
		{"get rayjobs", "get", "ray.io", "rayjobs", ""},
		{"list rayjobs", "list", "ray.io", "rayjobs", ""},
		{"get rayservices", "get", "ray.io", "rayservices", ""},
		{"list rayservices", "list", "ray.io", "rayservices", ""},
		{"list pods", "list", "", "pods", ""},
		{"get pods", "get", "", "pods", ""},
		{"list events", "list", "", "events", ""},
		{"get events", "get", "", "events", ""},
	}
	for _, tc := range allow {
		if !allowed(ctx, t, c, tc.verb, tc.group, tc.resource, tc.sub) {
			t.Errorf("DENIED but should be ALLOWED: %s (the read-only floor must grant the reads the tools make)", tc.name)
		}
	}

	// DENIED: writes on the read resources, writes on core, and the sensitive
	// negatives (secrets, pods/exec, pods/portforward, nodes, cluster-scoped
	// secrets) — proving the floor is genuinely tight, not "read + a little".
	deny := []struct {
		name                       string
		verb, group, resource, sub string
	}{
		{"create rayclusters", "create", "ray.io", "rayclusters", ""},
		{"update rayclusters", "update", "ray.io", "rayclusters", ""},
		{"patch rayclusters", "patch", "ray.io", "rayclusters", ""},
		{"delete rayclusters", "delete", "ray.io", "rayclusters", ""},
		{"create rayjobs", "create", "ray.io", "rayjobs", ""},
		{"delete rayservices", "delete", "ray.io", "rayservices", ""},
		{"create pods", "create", "", "pods", ""},
		{"delete pods", "delete", "", "pods", ""},
		{"create events", "create", "", "events", ""},
		{"get secrets", "get", "", "secrets", ""},
		{"list secrets", "list", "", "secrets", ""},
		{"create pods/exec", "create", "", "pods", "exec"},
		{"create pods/portforward", "create", "", "pods", "portforward"},
		{"list nodes", "list", "", "nodes", ""},
	}
	for _, tc := range deny {
		if allowed(ctx, t, c, tc.verb, tc.group, tc.resource, tc.sub) {
			t.Errorf("ALLOWED but should be DENIED: %s (the read-only floor must refuse writes and sensitive verbs)", tc.name)
		}
	}
}
