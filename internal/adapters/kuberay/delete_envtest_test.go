//go:build envtest

// Tier 2 (envtest) coverage for the RayCluster DELETE path (Task 12): the full
// two-call flow (preview → commit), protected-refusal, and dry-run validation
// against a real kube-apiserver + etcd + KubeRay CRDs. It proves end-to-end:
//   - a preview yields a non-empty fingerprint matching domain.Fingerprint of the
//     live object;
//   - a commit with that fingerprint deletes the cluster (GetCluster → NotFound);
//   - a protected cluster is refused (both preview and commit);
//   - a dry-run delete validates without removing the cluster.
//
// There is NO KubeRay operator here, so .status is never reconciled; these tests
// assert on existence/absence after the delete operation.
package kuberay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/risjai/ray-mcp/internal/domain"
)

// TestDeleteTwoCallFlow creates a RayCluster (via the curated-create helper),
// previews (empty confirm → get fingerprint), commits (confirm=fingerprint), and
// asserts the cluster is gone (GetCluster → NotFound). Also verifies the preview
// fingerprint is non-empty and equals domain.Fingerprint of the live object.
func TestDeleteTwoCallFlow(t *testing.T) {
	adapter, _ := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "delete-flow"
	svc := newClusterWriteService(t, adapter)

	// Create the cluster.
	if _, err := svc.Create(ctx, curatedCreateParams(namespace, name, nil)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Step 1: preview (empty confirm) → ConfirmRequiredError with fingerprint.
	err := svc.Delete(ctx, domain.ClusterDeleteParams{Namespace: namespace, Name: name})
	var required *domain.ConfirmRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("preview error = %v, want *ConfirmRequiredError", err)
	}
	if required.Fingerprint == "" {
		t.Fatal("preview fingerprint is empty")
	}

	// Verify the fingerprint matches domain.Fingerprint of the live object.
	detail, err := adapter.GetCluster(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}
	wantFP := domain.Fingerprint(detail.Raw, domain.OpDelete)
	if required.Fingerprint != wantFP {
		t.Errorf("preview fingerprint = %q, want %q (derived from the live object)", required.Fingerprint, wantFP)
	}
	// Sanity: the UID is present (so the fingerprint is non-trivial).
	meta, _ := detail.Raw["metadata"].(map[string]any)
	if uid, _ := meta["uid"].(string); uid == "" {
		t.Error("live metadata.uid is empty; fingerprint should be non-trivial")
	}

	// Step 2: commit with the correct fingerprint → cluster gone.
	if err := svc.Delete(ctx, domain.ClusterDeleteParams{Namespace: namespace, Name: name, Confirm: required.Fingerprint}); err != nil {
		t.Fatalf("commit Delete: %v", err)
	}

	// GetCluster should now return NotFound.
	_, err = adapter.GetCluster(ctx, namespace, name)
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("GetCluster after delete = %v, want *NotFoundError", err)
	}
}

// TestDeleteProtectedRefuses creates a RayCluster with ray-mcp/protected=true and
// asserts both preview and commit are refused. The cluster must still be present
// afterward.
func TestDeleteProtectedRefuses(t *testing.T) {
	adapter, _ := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "delete-protected"
	svc := newClusterWriteService(t, adapter)

	// Create the cluster with the protected annotation.
	p := curatedCreateParams(namespace, name, nil)
	p.Annotations = map[string]string{domain.ProtectedAnnotation: "true"}
	if _, err := svc.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Preview (empty confirm) → refused (not a ConfirmRequiredError).
	err := svc.Delete(ctx, domain.ClusterDeleteParams{Namespace: namespace, Name: name})
	if err == nil {
		t.Fatal("Delete (preview) on protected cluster returned nil, want error")
	}
	var required *domain.ConfirmRequiredError
	if errors.As(err, &required) {
		t.Fatal("protected cluster yielded a ConfirmRequiredError; must refuse before confirm")
	}

	// Commit with a (would-be) fingerprint → still refused.
	detail, _ := adapter.GetCluster(ctx, namespace, name)
	fp := domain.Fingerprint(detail.Raw, domain.OpDelete)
	err = svc.Delete(ctx, domain.ClusterDeleteParams{Namespace: namespace, Name: name, Confirm: fp})
	if err == nil {
		t.Fatal("Delete (commit) on protected cluster returned nil, want error")
	}

	// Cluster is still present.
	if _, getErr := adapter.GetCluster(ctx, namespace, name); getErr != nil {
		t.Errorf("cluster was deleted despite being protected: %v", getErr)
	}
}

// TestDeleteDryRunValidatesWithoutDeleting creates a cluster, confirms a delete
// with dryRun=true, and asserts the cluster is still present afterward (dry-run
// validates but persists nothing).
func TestDeleteDryRunValidatesWithoutDeleting(t *testing.T) {
	adapter, _ := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "delete-dryrun"
	svc := newClusterWriteService(t, adapter)

	if _, err := svc.Create(ctx, curatedCreateParams(namespace, name, nil)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Get the fingerprint via preview.
	err := svc.Delete(ctx, domain.ClusterDeleteParams{Namespace: namespace, Name: name})
	var required *domain.ConfirmRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("preview error = %v, want *ConfirmRequiredError", err)
	}

	// Commit with dryRun=true.
	if err := svc.Delete(ctx, domain.ClusterDeleteParams{
		Namespace: namespace, Name: name, Confirm: required.Fingerprint, DryRun: true,
	}); err != nil {
		t.Fatalf("Delete(dryRun): %v", err)
	}

	// Cluster is still present.
	if _, getErr := adapter.GetCluster(ctx, namespace, name); getErr != nil {
		t.Errorf("cluster was deleted despite dryRun=true: %v", getErr)
	}
}
