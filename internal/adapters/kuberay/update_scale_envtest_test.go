//go:build envtest

// Tier 2 (envtest) coverage for the RayCluster UPDATE/SCALE path (Task 10, F13 —
// the highest test-risk case in the cluster track). envtest runs a real
// kube-apiserver + etcd + the installed KubeRay CRD, but NO operator and NO Ray
// autoscaler. So "the autoscaler owns replicas" is simulated by a SECOND field
// manager that writes replicas, and we prove against the REAL apiserver's
// Server-Side Apply semantics:
//
//   - the read-modify-apply-full path changes one field (image/min/max) without
//     pruning the rest of the spec ray-mcp owns;
//   - because spec.workerGroupSpecs is an ATOMIC SSA list (the installed CRD
//     declares no x-kubernetes-list-type), update preserves the LIVE replicas
//     value rather than resetting it — the autoscaler's count is not clobbered;
//   - a genuine field-ownership conflict from another APPLY manager surfaces as a
//     domain.ConflictError (we do not force-steal).
//
// These tests are the empirical proof of the lost-update guard the plan budgets
// extra time for; the autoscaler mechanism (JSON Patch, atomic list) was verified
// against KubeRay v1.6.1 upstream and is asserted here against the live schema.
package kuberay

import (
	"context"
	"fmt"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// seedCluster applies a full RayCluster as the ray-mcp manager (the create path),
// returning the write service for the subsequent update/scale. autoscaling toggles
// spec.enableInTreeAutoscaling on the seeded object.
func seedCluster(ctx context.Context, t *testing.T, adapter *Client, namespace, name string, autoscaling bool) *domain.ClusterWriteService {
	t.Helper()
	svc := newClusterWriteService(t, adapter)
	p := curatedCreateParams(namespace, name, nil)
	p.EnableAutoscaling = autoscaling
	if autoscaling {
		// Under autoscaling the curated builder still sets replicas; that is the
		// create-time desired count. min/max bound the autoscaler.
		p.WorkerGroups[0].MinReplicas = 1
		p.WorkerGroups[0].MaxReplicas = 5
	}
	if _, err := svc.Create(ctx, p); err != nil {
		t.Fatalf("seed create: %v", err)
	}
	return svc
}

// newClusterWriteService is defined in create_envtest_test.go (same package).

// autoscalerSetsReplicas simulates the Ray autoscaler writing replicas the way the
// real autoscaler does: a JSON Patch (application/json-patch+json) targeting ONLY
// spec.workerGroupSpecs[idx].replicas, attributed to a distinct field manager. A
// JSON Patch is an Update-type write (not an Apply), so the autoscaler becomes the
// managed-fields owner of replicas without owning the rest of the object — exactly
// the contention ray-mcp must survive. groupIdx is the worker group's index.
func autoscalerSetsReplicas(ctx context.Context, t *testing.T, k8s client.Client, namespace, name string, groupIdx, replicas int) {
	t.Helper()
	patch := []byte(fmt.Sprintf(
		`[{"op":"replace","path":"/spec/workerGroupSpecs/%d/replicas","value":%d}]`, groupIdx, replicas))
	rc := &rayv1.RayCluster{}
	rc.SetNamespace(namespace)
	rc.SetName(name)
	if err := k8s.Patch(ctx, rc, client.RawPatch(types.JSONPatchType, patch), client.FieldOwner("ray-autoscaler")); err != nil {
		t.Fatalf("autoscaler JSON patch: %v", err)
	}
}

// TestUpdateImagePreservesWorkerReplicas is the core F13 AC: after the autoscaler
// has driven replicas to 4, a ray-mcp update of the image must leave replicas at 4
// (read-modify-apply-full preserves the live value; the atomic list means we must
// resend it, and we resend what we read — the autoscaler's value).
func TestUpdateImagePreservesWorkerReplicas(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "update-preserve"
	svc := seedCluster(ctx, t, adapter, namespace, name, false)

	// Simulate the autoscaler scaling workers up to 4 out from under ray-mcp.
	autoscalerSetsReplicas(ctx, t, k8s, namespace, name, 0, 4)

	// ray-mcp updates only the image.
	if _, err := svc.Update(ctx, domain.ClusterUpdateParams{
		Namespace: namespace, Name: name, Image: "rayproject/ray:2.40.0",
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	rc, err := getRayCluster(ctx, t, k8s, namespace, name)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if len(rc.Spec.WorkerGroupSpecs) != 1 {
		t.Fatalf("worker groups = %d, want 1 (update must not prune the group)", len(rc.Spec.WorkerGroupSpecs))
	}
	if got := deref(rc.Spec.WorkerGroupSpecs[0].Replicas); got != 4 {
		t.Errorf("replicas = %d after image update, want 4 preserved (the autoscaler's value must not be clobbered)", got)
	}
	// The image change landed on the worker container.
	wc := rc.Spec.WorkerGroupSpecs[0].Template.Spec.Containers
	if len(wc) == 0 || wc[0].Image != "rayproject/ray:2.40.0" {
		t.Errorf("worker image = %v, want the updated image", wc)
	}
}

// TestUpdatePreservesAutoscalingReplicas is the autoscaling variant: on an
// autoscaling cluster, after the autoscaler sets replicas, a ray-mcp update still
// leaves the live replicas intact (we resend what we read; we never zero it).
func TestUpdatePreservesAutoscalingReplicas(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "update-autoscale"
	svc := seedCluster(ctx, t, adapter, namespace, name, true)

	autoscalerSetsReplicas(ctx, t, k8s, namespace, name, 0, 3)

	if _, err := svc.Update(ctx, domain.ClusterUpdateParams{
		Namespace: namespace, Name: name, Image: "rayproject/ray:2.40.0",
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	rc, err := getRayCluster(ctx, t, k8s, namespace, name)
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if got := deref(rc.Spec.WorkerGroupSpecs[0].Replicas); got != 3 {
		t.Errorf("replicas = %d after autoscaling update, want 3 preserved", got)
	}
}

// TestScaleMinMaxPreservesReplicas asserts a scale of min/max preserves the
// autoscaler-set replicas on the same group (read-modify-apply-full again).
func TestScaleMinMaxPreservesReplicas(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "scale-minmax"
	svc := seedCluster(ctx, t, adapter, namespace, name, true)
	autoscalerSetsReplicas(ctx, t, k8s, namespace, name, 0, 4)

	if _, err := svc.Scale(ctx, domain.ClusterScaleParams{
		Namespace: namespace, Name: name, WorkerGroup: "workers", MaxReplicas: i32e(10),
	}); err != nil {
		t.Fatalf("Scale: %v", err)
	}

	rc, err := getRayCluster(ctx, t, k8s, namespace, name)
	if err != nil {
		t.Fatalf("get after scale: %v", err)
	}
	wg := rc.Spec.WorkerGroupSpecs[0]
	if deref(wg.MaxReplicas) != 10 {
		t.Errorf("maxReplicas = %d, want 10", deref(wg.MaxReplicas))
	}
	if deref(wg.Replicas) != 4 {
		t.Errorf("replicas = %d after min/max scale, want 4 preserved", deref(wg.Replicas))
	}
}

// TestScaleRecoversFromConflictViaForceRetry is the lost-update guard end-to-end:
// when ANOTHER apply manager owns the atomic worker list, ray-mcp's first
// (non-force) scale apply gets a 409 — the adapter maps it to ConflictError — and
// the domain force-retries ONCE from a fresh read, which wins. The scale therefore
// succeeds (no error to the caller) and ray-mcp's value lands. This is the §7.D
// "retry once only when the change is ours" behavior against a real apiserver.
func TestScaleRecoversFromConflictViaForceRetry(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "scale-conflict"
	svc := seedCluster(ctx, t, adapter, namespace, name, false)

	// A competing APPLY manager force-claims ownership of the worker group (atomic
	// list → owning the whole element) with its own maxReplicas. Force is needed
	// because ray-mcp's seed already owns the list; a real co-owning controller
	// would likewise force.
	rival := clusterMergedSpec(namespace, name, nil)
	rivalGroups := rival["spec"].(map[string]any)["workerGroupSpecs"].([]any)
	rivalGroups[0].(map[string]any)["maxReplicas"] = int64(99)
	rivalObj := &unstructured.Unstructured{Object: rival}
	rivalObj.SetGroupVersionKind(rayv1.GroupVersion.WithKind("RayCluster"))
	if err := k8s.Apply(ctx, client.ApplyConfigurationFromUnstructured(rivalObj),
		client.FieldOwner("rival-controller"), client.ForceOwnership); err != nil {
		t.Fatalf("rival apply: %v", err)
	}

	// ray-mcp scales maxReplicas to a DIFFERENT value. The first apply conflicts
	// (rival owns the atomic list); the force-retry wins.
	if _, err := svc.Scale(ctx, domain.ClusterScaleParams{
		Namespace: namespace, Name: name, WorkerGroup: "workers", MaxReplicas: i32e(7),
	}); err != nil {
		t.Fatalf("Scale should recover from the conflict via force-retry, got: %v", err)
	}

	rc, err := getRayCluster(ctx, t, k8s, namespace, name)
	if err != nil {
		t.Fatalf("get after scale: %v", err)
	}
	if got := deref(rc.Spec.WorkerGroupSpecs[0].MaxReplicas); got != 7 {
		t.Errorf("maxReplicas = %d after force-retry, want 7 (ray-mcp's value won)", got)
	}
}

// TestScaleToZeroEndToEnd proves a scale-to-zero (destructive tier) persists
// replicas=0 on a non-autoscaling cluster — the worker group is drained but its
// spec remains (the head stays).
func TestScaleToZeroEndToEnd(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "scale-zero"
	svc := seedCluster(ctx, t, adapter, namespace, name, false)

	if _, err := svc.Scale(ctx, domain.ClusterScaleParams{
		Namespace: namespace, Name: name, WorkerGroup: "workers",
		Replicas: i32e(0), AllowDestructive: true,
	}); err != nil {
		t.Fatalf("scale-to-zero: %v", err)
	}

	rc, err := getRayCluster(ctx, t, k8s, namespace, name)
	if err != nil {
		t.Fatalf("get after scale-to-zero: %v", err)
	}
	if len(rc.Spec.WorkerGroupSpecs) != 1 {
		t.Fatalf("worker groups = %d, want 1 (scale-to-zero drains, does not delete the group)", len(rc.Spec.WorkerGroupSpecs))
	}
	if got := deref(rc.Spec.WorkerGroupSpecs[0].Replicas); got != 0 {
		t.Errorf("replicas = %d, want 0 after scale-to-zero", got)
	}
}

// i32e returns a pointer to an int32 (envtest-local helper; the domain tests use
// i32 in the same-package _test file, which is a different package here).
func i32e(v int32) *int32 { return &v }
