//go:build envtest

// Tier 2 (envtest) coverage for the RayJob DELETE path (Task 19, Q16a), the
// mode-aware destructive tiering, against a real kube-apiserver + etcd + KubeRay
// CRDs. It proves end-to-end, against the INSTALLED CRD schema:
//   - an EPHEMERAL job (spec.rayClusterSpec) is the destructive tier: the full
//     two-call flow (preview → fingerprint matching domain.Fingerprint → commit)
//     deletes it, and it is refused without the destructive tier;
//   - an EXISTING-CLUSTER job (spec.clusterSelector) is a plain write: it deletes
//     with no tier and no confirm;
//   - the ray-mcp/protected annotation refuses regardless of mode;
//   - a dry-run validates without deleting.
//
// There is NO KubeRay operator here, so nothing reconciles or cascades; these
// tests assert on the RayJob record's existence/absence after the operation. The
// cascade itself (ephemeral cluster teardown) is KubeRay's job via owner refs and
// is out of scope for the adapter contract.
package kuberay

import (
	"context"
	"errors"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/risjai/ray-mcp/internal/domain"
)

// newExistingClusterRayJob builds a minimal valid existing-cluster RayJob: it
// targets a cluster by name via spec.clusterSelector and carries NO embedded
// rayClusterSpec (the existing-cluster mode the submit path persists). The CRD
// accepts a RayJob with a clusterSelector and no embedded cluster.
func newExistingClusterRayJob(namespace, name string) *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: rayv1.RayJobSpec{
			Entrypoint:      "python -c 'import ray; ray.init()'",
			ClusterSelector: map[string]string{"ray.io/cluster": name + "-target"},
		},
	}
}

// TestJobDeleteEphemeralTwoCallFlow creates an ephemeral RayJob, previews (empty
// confirm + the destructive tier → fingerprint), verifies the fingerprint matches
// domain.Fingerprint of the live object, commits, and asserts the job is gone.
func TestJobDeleteEphemeralTwoCallFlow(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "ephem-delete-flow"
	svc := newJobWriteService(t, adapter)

	if err := k8s.Create(ctx, newRayJob(namespace, name)); err != nil {
		t.Fatalf("create ephemeral RayJob: %v", err)
	}

	// Step 1: preview (empty confirm) WITH the destructive tier → ConfirmRequiredError.
	err := svc.Delete(ctx, domain.JobDeleteParams{Namespace: namespace, Name: name, AllowDestructive: true})
	var required *domain.ConfirmRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("preview error = %v, want *ConfirmRequiredError", err)
	}
	if required.Fingerprint == "" {
		t.Fatal("preview fingerprint is empty")
	}

	detail, err := adapter.GetJob(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if wantFP := domain.Fingerprint(detail.Raw, domain.OpDelete); required.Fingerprint != wantFP {
		t.Errorf("preview fingerprint = %q, want %q (derived from the live object)", required.Fingerprint, wantFP)
	}
	meta, _ := detail.Raw["metadata"].(map[string]any)
	if uid, _ := meta["uid"].(string); uid == "" {
		t.Error("live metadata.uid is empty; fingerprint should be non-trivial")
	}

	// Step 2: commit with the correct fingerprint → job gone.
	if err := svc.Delete(ctx, domain.JobDeleteParams{Namespace: namespace, Name: name, Confirm: required.Fingerprint, AllowDestructive: true}); err != nil {
		t.Fatalf("commit Delete: %v", err)
	}
	if _, err := adapter.GetJob(ctx, namespace, name); !errors.As(err, new(*domain.NotFoundError)) {
		t.Fatalf("GetJob after delete = %v, want *NotFoundError", err)
	}
}

// TestJobDeleteEphemeralRequiresDestructiveTier asserts an ephemeral job is refused
// without the destructive tier — before any fingerprint is minted — and the job
// is still present afterward.
func TestJobDeleteEphemeralRequiresDestructiveTier(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "ephem-no-tier"
	svc := newJobWriteService(t, adapter)

	if err := k8s.Create(ctx, newRayJob(namespace, name)); err != nil {
		t.Fatalf("create ephemeral RayJob: %v", err)
	}

	err := svc.Delete(ctx, domain.JobDeleteParams{Namespace: namespace, Name: name, AllowDestructive: false})
	if err == nil {
		t.Fatal("ephemeral Delete without the destructive tier returned nil, want a refusal")
	}
	if errors.As(err, new(*domain.ConfirmRequiredError)) {
		t.Fatal("ephemeral Delete without the tier yielded a ConfirmRequiredError; the tier gate must precede the confirm preview")
	}
	if _, getErr := adapter.GetJob(ctx, namespace, name); getErr != nil {
		t.Errorf("ephemeral job was deleted without the destructive tier: %v", getErr)
	}
}

// TestJobDeleteExistingClusterPlainDelete asserts an existing-cluster job deletes
// immediately as a plain write — no destructive tier, no confirm — and is gone
// afterward.
func TestJobDeleteExistingClusterPlainDelete(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "attached-delete"
	svc := newJobWriteService(t, adapter)

	if err := k8s.Create(ctx, newExistingClusterRayJob(namespace, name)); err != nil {
		t.Fatalf("create existing-cluster RayJob: %v", err)
	}

	// Empty confirm, no tier → deletes straight away.
	if err := svc.Delete(ctx, domain.JobDeleteParams{Namespace: namespace, Name: name, AllowDestructive: false}); err != nil {
		t.Fatalf("existing-cluster Delete: %v", err)
	}
	if _, err := adapter.GetJob(ctx, namespace, name); !errors.As(err, new(*domain.NotFoundError)) {
		t.Fatalf("GetJob after delete = %v, want *NotFoundError", err)
	}
}

// TestJobDeleteProtectedRefuses asserts the ray-mcp/protected annotation refuses a
// delete regardless of mode (here an existing-cluster job, the plain-write path —
// the guard must still fire), and the job is still present afterward.
func TestJobDeleteProtectedRefuses(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "protected-job"
	svc := newJobWriteService(t, adapter)

	rj := newExistingClusterRayJob(namespace, name)
	rj.Annotations = map[string]string{domain.ProtectedAnnotation: "true"}
	if err := k8s.Create(ctx, rj); err != nil {
		t.Fatalf("create protected RayJob: %v", err)
	}

	err := svc.Delete(ctx, domain.JobDeleteParams{Namespace: namespace, Name: name})
	if err == nil {
		t.Fatal("Delete on protected job returned nil, want a refusal")
	}
	if _, getErr := adapter.GetJob(ctx, namespace, name); getErr != nil {
		t.Errorf("job was deleted despite being protected: %v", getErr)
	}
}

// TestJobDeleteDryRunValidatesWithoutDeleting asserts dryRun=true on the plain
// existing-cluster path validates server-side but persists nothing (the job is
// still present afterward).
func TestJobDeleteDryRunValidatesWithoutDeleting(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "dryrun-delete"
	svc := newJobWriteService(t, adapter)

	if err := k8s.Create(ctx, newExistingClusterRayJob(namespace, name)); err != nil {
		t.Fatalf("create existing-cluster RayJob: %v", err)
	}

	if err := svc.Delete(ctx, domain.JobDeleteParams{Namespace: namespace, Name: name, DryRun: true}); err != nil {
		t.Fatalf("Delete(dryRun): %v", err)
	}
	if _, getErr := adapter.GetJob(ctx, namespace, name); getErr != nil {
		t.Errorf("job was deleted despite dryRun=true: %v", getErr)
	}
}
