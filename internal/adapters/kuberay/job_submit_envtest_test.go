//go:build envtest

// Tier 2 (envtest) coverage for the RayJob SUBMIT path (Task 18): the curated job
// base builder (BuildJobBase) composed with the full domain JobWriteService (Merge
// + ApplyService) against a real kube-apiserver + etcd + KubeRay CRDs. It proves
// end-to-end, against the INSTALLED CRD schema (not a fake):
//   - existing-cluster mode persists a RayJob with spec.clusterSelector and NO
//     embedded rayClusterSpec, the apiserver accepts;
//   - ephemeral mode (clusterSpec) persists a RayJob with spec.rayClusterSpec and
//     the Q16b shutdownAfterJobFinishes=true default, NO clusterSelector;
//   - dryRun validates but persists nothing;
//   - the mode XOR (both / neither) is a client-side validation error before any
//     apply, so nothing is persisted.
//
// There is NO KubeRay operator here, so .status is never reconciled — these tests
// assert on what the apiserver owns (spec persistence, schema validation).
package kuberay

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/observability"
)

// newJobWriteService wires the real envtest-backed adapter into the full domain
// job-write pipeline (JobBaseBuilder + JobGetter + Deleter + ApplyService), so a
// submit exercises curated→base→merge→DryRunAll→SSA→diff→audit and a delete
// exercises the mode-aware confirm/cascade path against a real apiserver.
func newJobWriteService(t *testing.T, adapter *Client) *domain.JobWriteService {
	t.Helper()
	apply := domain.NewApplyService(adapter, observability.NewAuditLogger(discardWriter{}))
	return domain.NewJobWriteService(adapter, adapter, adapter, apply, "default")
}

// getRayJob fetches a RayJob directly (bypassing the adapter) so a test can assert
// what was actually persisted, or that nothing was.
func getRayJob(ctx context.Context, t *testing.T, k8s client.Client, namespace, name string) (*rayv1.RayJob, error) {
	t.Helper()
	var rj rayv1.RayJob
	err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rj)
	return &rj, err
}

// curatedEphemeralSpec is a minimal valid curated ephemeral cluster spec: image +
// one worker group, head/worker resources, so the embedded rayClusterSpec passes
// the CRD schema's required-field validation.
func curatedEphemeralSpec() *domain.ClusterSubmitSpec {
	return &domain.ClusterSubmitSpec{
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 1, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
	}
}

// TestSubmitExistingClusterPersists is the existing-cluster mode AC: a curated
// submit targeting a cluster by name persists a RayJob whose spec carries
// clusterSelector["ray.io/cluster"] and NO embedded rayClusterSpec.
func TestSubmitExistingClusterPersists(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newJobWriteService(t, adapter)
	const namespace, name = "default", "existing-job"

	res, err := svc.Submit(ctx, domain.JobSubmitParams{
		Namespace:       namespace,
		Name:            name,
		Entrypoint:      "python main.py",
		ExistingCluster: "target-cluster",
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if res.DryRun {
		t.Error("result DryRun = true, want false for a committed submit")
	}
	if res.Ephemeral {
		t.Error("result Ephemeral = true, want false for existing-cluster mode")
	}

	rj, getErr := getRayJob(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("submitted job not gettable: %v", getErr)
	}
	if rj.Spec.Entrypoint != "python main.py" {
		t.Errorf("persisted entrypoint = %q, want python main.py", rj.Spec.Entrypoint)
	}
	if got := rj.Spec.ClusterSelector["ray.io/cluster"]; got != "target-cluster" {
		t.Errorf("clusterSelector[ray.io/cluster] = %q, want target-cluster", got)
	}
	if rj.Spec.RayClusterSpec != nil {
		t.Errorf("rayClusterSpec = %+v, want nil in existing-cluster mode", rj.Spec.RayClusterSpec)
	}
}

// TestSubmitEphemeralPersists is the ephemeral mode AC: a curated submit with a
// clusterSpec persists a RayJob whose spec carries an embedded rayClusterSpec
// (head + worker groups), the Q16b shutdownAfterJobFinishes=true default, and NO
// clusterSelector.
func TestSubmitEphemeralPersists(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newJobWriteService(t, adapter)
	const namespace, name = "default", "ephemeral-job"

	res, err := svc.Submit(ctx, domain.JobSubmitParams{
		Namespace:   namespace,
		Name:        name,
		Entrypoint:  "python main.py",
		ClusterSpec: curatedEphemeralSpec(),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if !res.Ephemeral {
		t.Error("result Ephemeral = false, want true for clusterSpec mode")
	}

	rj, getErr := getRayJob(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("submitted job not gettable: %v", getErr)
	}
	if rj.Spec.RayClusterSpec == nil {
		t.Fatal("persisted rayClusterSpec = nil, want an embedded cluster")
	}
	if rj.Spec.RayClusterSpec.RayVersion != "2.9.0" {
		t.Errorf("rayClusterSpec.rayVersion = %q, want 2.9.0", rj.Spec.RayClusterSpec.RayVersion)
	}
	if len(rj.Spec.RayClusterSpec.WorkerGroupSpecs) != 1 {
		t.Errorf("rayClusterSpec worker groups = %d, want 1", len(rj.Spec.RayClusterSpec.WorkerGroupSpecs))
	}
	// Q16b: the ephemeral default tears the cluster down on finish.
	if !rj.Spec.ShutdownAfterJobFinishes {
		t.Error("shutdownAfterJobFinishes = false, want true (Q16b ephemeral default)")
	}
	if len(rj.Spec.ClusterSelector) != 0 {
		t.Errorf("clusterSelector = %+v, want empty in ephemeral mode", rj.Spec.ClusterSelector)
	}
}

// TestSubmitEphemeralRespectsExplicitShutdownFalse asserts the "keep the cluster
// for debugging" path round-trips through a real apiserver: an explicit false is
// NOT overwritten by the true default and persists as false. (The unit test
// TestBuildJobBaseEphemeralExplicitShutdownFalse proves the upstream half — that
// the adapter carries the false on the wire despite the typed field's omitempty;
// the typed read-back here cannot distinguish sent-false from defaulted-false, so
// the two tests together cover the contract.)
func TestSubmitEphemeralRespectsExplicitShutdownFalse(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newJobWriteService(t, adapter)
	const namespace, name = "default", "ephemeral-keep"

	keep := false
	if _, err := svc.Submit(ctx, domain.JobSubmitParams{
		Namespace:                namespace,
		Name:                     name,
		Entrypoint:               "python main.py",
		ClusterSpec:              curatedEphemeralSpec(),
		ShutdownAfterJobFinishes: &keep,
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	rj, getErr := getRayJob(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("submitted job not gettable: %v", getErr)
	}
	if rj.Spec.ShutdownAfterJobFinishes {
		t.Error("shutdownAfterJobFinishes = true, want explicit false preserved")
	}
}

// TestSubmitDryRunPersistsNothing asserts dryRun=true validates against the CRD
// schema but persists nothing.
func TestSubmitDryRunPersistsNothing(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newJobWriteService(t, adapter)
	const namespace, name = "default", "dryrun-job"

	res, err := svc.Submit(ctx, domain.JobSubmitParams{
		Namespace:       namespace,
		Name:            name,
		Entrypoint:      "python main.py",
		ExistingCluster: "target-cluster",
		DryRun:          true,
	})
	if err != nil {
		t.Fatalf("Submit(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if _, getErr := getRayJob(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("after dry-run the job exists (get err = %v), want NotFound", getErr)
	}
}

// TestSubmitModeXORRejectedBeforeApply asserts both-modes and neither-mode are
// client-side validation errors before any apply, so nothing is persisted against
// the apiserver.
func TestSubmitModeXORRejectedBeforeApply(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newJobWriteService(t, adapter)
	const namespace = "default"

	// Both modes set.
	if _, err := svc.Submit(ctx, domain.JobSubmitParams{
		Namespace: namespace, Name: "both-job", Entrypoint: "python main.py",
		ExistingCluster: "c", ClusterSpec: curatedEphemeralSpec(),
	}); err == nil {
		t.Error("Submit with both modes returned nil, want a validation error")
	}
	if _, getErr := getRayJob(ctx, t, k8s, namespace, "both-job"); !apierrors.IsNotFound(getErr) {
		t.Errorf("both-mode submit persisted a job (get err = %v), want NotFound", getErr)
	}

	// Neither mode set.
	if _, err := svc.Submit(ctx, domain.JobSubmitParams{
		Namespace: namespace, Name: "neither-job", Entrypoint: "python main.py",
	}); err == nil {
		t.Error("Submit with neither mode returned nil, want a validation error")
	}
	if _, getErr := getRayJob(ctx, t, k8s, namespace, "neither-job"); !apierrors.IsNotFound(getErr) {
		t.Errorf("neither-mode submit persisted a job (get err = %v), want NotFound", getErr)
	}
}

// compile-time proof the production adapter satisfies the job-write backend half.
var _ domain.JobBaseBuilder = (*Client)(nil)
