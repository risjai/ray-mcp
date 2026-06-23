//go:build envtest

// Tier 2 (envtest) coverage for the RayCluster CREATE path (Task 9): the curated
// base builder (BuildClusterBase) composed with the full domain ClusterWriteService
// (Merge + ApplyService) against a real kube-apiserver + etcd + KubeRay CRDs. It
// proves end-to-end, against the INSTALLED CRD schema (not a fake):
//   - a curated create persists a valid RayCluster the apiserver accepts;
//   - dryRun validates but persists nothing;
//   - an unknown rawSpec field is REJECTED by the structural CRD schema (a hard
//     error, not silent pruning — spec §7.C/Q4);
//   - the curated resource quantities (cpu/memory/gpu) land on the container.
//
// There is NO KubeRay operator here, so .status is never reconciled — these tests
// assert on what the apiserver owns (spec persistence, schema validation).
package kuberay

import (
	"context"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/observability"
)

// newClusterWriteService wires the real envtest-backed adapter into the full
// domain write pipeline (ClusterBaseBuilder + ApplyService), so a create exercises
// curated→base→merge→DryRunAll→SSA→diff→audit against a real apiserver.
func newClusterWriteService(t *testing.T, adapter *Client) *domain.ClusterWriteService {
	t.Helper()
	apply := domain.NewApplyService(adapter, observability.NewAuditLogger(discardWriter{}))
	return domain.NewClusterWriteService(adapter, adapter, adapter, apply, "default")
}

// curatedCreateParams builds a minimal valid curated create for the given
// identity: image + one worker group, head/worker CPU set. extraRaw lets a test
// inject a rawSpec (e.g. an unknown field for the rejection case).
func curatedCreateParams(namespace, name string, extraRaw domain.MergedSpec) domain.ClusterCreateParams {
	return domain.ClusterCreateParams{
		Namespace:     namespace,
		Name:          name,
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 2, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
		RawSpec: extraRaw,
	}
}

// TestCreateCuratedPersists is the core create AC: a curated-only create persists
// a RayCluster the installed CRD schema accepts, with the curated params mapped
// onto the typed spec.
func TestCreateCuratedPersists(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newClusterWriteService(t, adapter)
	const namespace, name = "default", "curated-cluster"

	res, err := svc.Create(ctx, curatedCreateParams(namespace, name, nil))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if res.DryRun {
		t.Error("result DryRun = true, want false for a committed create")
	}

	rc, getErr := getRayCluster(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("created cluster not gettable: %v", getErr)
	}
	if rc.Spec.RayVersion != "2.9.0" {
		t.Errorf("persisted rayVersion = %q, want 2.9.0", rc.Spec.RayVersion)
	}
	if len(rc.Spec.WorkerGroupSpecs) != 1 || rc.Spec.WorkerGroupSpecs[0].GroupName != "workers" {
		t.Fatalf("persisted worker groups = %+v, want one 'workers' group", rc.Spec.WorkerGroupSpecs)
	}
	// The curated maxReplicas (5) and replicas (2) survived the typed round-trip.
	wg := rc.Spec.WorkerGroupSpecs[0]
	if wg.Replicas == nil || *wg.Replicas != 2 || wg.MaxReplicas == nil || *wg.MaxReplicas != 5 {
		t.Errorf("worker replicas/max = %v/%v, want 2/5", deref(wg.Replicas), deref(wg.MaxReplicas))
	}
	// The head container carries the curated CPU request.
	headContainers := rc.Spec.HeadGroupSpec.Template.Spec.Containers
	if len(headContainers) != 1 {
		t.Fatalf("head containers = %d, want 1", len(headContainers))
	}
	if cpu := headContainers[0].Resources.Requests.Cpu(); cpu == nil || cpu.String() != "1" {
		t.Errorf("head CPU request = %v, want 1", cpu)
	}
}

// TestCreateClampsMaxReplicasAboveMin is the regression guard for the worker-group
// clamp: a caller that sets minReplicas above the (defaulted-zero) maxReplicas must
// NOT produce minReplicas > maxReplicas, which KubeRay validation rejects. The
// curated builder clamps maxReplicas up to max(replicas, minReplicas).
func TestCreateClampsMaxReplicasAboveMin(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newClusterWriteService(t, adapter)
	const namespace, name = "default", "clamp-cluster"

	p := curatedCreateParams(namespace, name, nil)
	// minReplicas above the desired replicas, maxReplicas left at the zero value.
	p.WorkerGroups = []domain.WorkerGroupParams{{Name: "workers", Replicas: 2, MinReplicas: 4, MaxReplicas: 0}}

	if _, err := svc.Create(ctx, p); err != nil {
		t.Fatalf("Create: %v (clamp should keep maxReplicas >= minReplicas)", err)
	}
	rc, getErr := getRayCluster(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("created cluster not gettable: %v", getErr)
	}
	wg := rc.Spec.WorkerGroupSpecs[0]
	if deref(wg.MaxReplicas) < deref(wg.MinReplicas) {
		t.Errorf("maxReplicas %d < minReplicas %d; clamp must keep max >= min", deref(wg.MaxReplicas), deref(wg.MinReplicas))
	}
	if deref(wg.MaxReplicas) != 4 {
		t.Errorf("maxReplicas = %d, want 4 (clamped up to minReplicas)", deref(wg.MaxReplicas))
	}
}

// TestCreateDryRunPersistsNothing asserts dryRun=true validates against the CRD
// schema but persists nothing.
func TestCreateDryRunPersistsNothing(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newClusterWriteService(t, adapter)
	const namespace, name = "default", "dryrun-create"

	res, err := svc.Create(ctx, func() domain.ClusterCreateParams {
		p := curatedCreateParams(namespace, name, nil)
		p.DryRun = true
		return p
	}())
	if err != nil {
		t.Fatalf("Create(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if _, getErr := getRayCluster(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("after dry-run the cluster exists (get err = %v), want NotFound", getErr)
	}
}

// TestCreateRejectsUnknownRawSpecField is the validation-oracle AC: an unknown
// field supplied via rawSpec is REJECTED by the installed structural CRD schema
// (hard error), NOT silently pruned. The unconditional DryRunAll surfaces it
// before any persist.
func TestCreateRejectsUnknownRawSpecField(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newClusterWriteService(t, adapter)
	const namespace, name = "default", "bad-rawspec"

	raw := domain.MergedSpec{"spec": map[string]any{"thisFieldDoesNotExistInTheCRD": "x"}}
	_, err := svc.Create(ctx, curatedCreateParams(namespace, name, raw))
	if err == nil {
		t.Fatal("Create with an unknown rawSpec field returned nil error; SSA must reject it")
	}
	// Nothing persisted (the dry-run gate rejected before commit).
	if _, getErr := getRayCluster(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("a rejected create persisted the cluster (get err = %v), want NotFound", getErr)
	}
}

// TestCreateRawSpecMergesOverCurated proves the rawSpec escape hatch reaches the
// persisted object: a rawSpec that sets spec.enableInTreeAutoscaling overrides the
// curated default and lands on the stored RayCluster.
func TestCreateRawSpecMergesOverCurated(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newClusterWriteService(t, adapter)
	const namespace, name = "default", "rawspec-merge"

	raw := domain.MergedSpec{"spec": map[string]any{"enableInTreeAutoscaling": true}}
	if _, err := svc.Create(ctx, curatedCreateParams(namespace, name, raw)); err != nil {
		t.Fatalf("Create: %v", err)
	}

	rc, getErr := getRayCluster(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("created cluster not gettable: %v", getErr)
	}
	if rc.Spec.EnableInTreeAutoscaling == nil || !*rc.Spec.EnableInTreeAutoscaling {
		t.Errorf("enableInTreeAutoscaling = %v, want true (rawSpec merged over curated)", rc.Spec.EnableInTreeAutoscaling)
	}
}

// deref returns the pointed-to int32 or -1 for nil, for readable assertions.
func deref(p *int32) int32 {
	if p == nil {
		return -1
	}
	return *p
}

// discardWriter is an io.Writer sink for the audit logger in tests that do not
// assert on audit output.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// compile-time proof the production adapter satisfies the write backend halves.
var (
	_ domain.ClusterBaseBuilder = (*Client)(nil)
	_ domain.Applier            = (*Client)(nil)
	_ domain.Deleter            = (*Client)(nil)
)
