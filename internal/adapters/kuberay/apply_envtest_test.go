//go:build envtest

// Tier 2 (envtest) coverage for the KubeRay adapter's APPLY path (Task 8b). It
// boots the same real kube-apiserver + etcd + KubeRay CRDs harness as the read
// tests (startAdapter, newRayCluster live in envtest_test.go) and exercises
// Server-Side Apply end-to-end:
//   - DryRunAll validates but PERSISTS NOTHING (the object stays absent);
//   - a real apply creates the object and the read-back carries server-defaulted
//     fields, which the diff surfaces;
//   - an unknown spec field is REJECTED by the installed CRD schema (a hard error,
//     not silent pruning — SSA against a structural CRD rejects) and detected;
//   - the full domain ApplyService drives the same adapter (the choke point that
//     emits audit) without a fake.
//
// There is NO KubeRay operator here, so .status is never reconciled — these tests
// assert on spec/persistence/pruning, which the apiserver owns, not on status.
package kuberay

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// clusterMergedSpec builds the unstructured RayCluster the apply pipeline hands
// the adapter: a minimal valid spec (head + one worker group) plus the identity
// metadata. It mirrors newRayCluster's shape as a plain map so SSA validates it
// against the installed CRD schema. extraSpec lets a test inject an extra spec
// key (e.g. an unknown field for the pruning case).
func clusterMergedSpec(namespace, name string, extraSpec map[string]any) domain.MergedSpec {
	spec := map[string]any{
		"rayVersion": "2.9.0",
		"headGroupSpec": map[string]any{
			"rayStartParams": map[string]any{},
			"template": map[string]any{
				"spec": map[string]any{
					"containers": []any{
						map[string]any{"name": "ray-head", "image": "rayproject/ray:2.9.0"},
					},
				},
			},
		},
		"workerGroupSpecs": []any{
			map[string]any{
				"groupName":      "workers",
				"replicas":       int64(2),
				"minReplicas":    int64(0),
				"maxReplicas":    int64(5),
				"rayStartParams": map[string]any{},
				"template": map[string]any{
					"spec": map[string]any{
						"containers": []any{
							map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0"},
						},
					},
				},
			},
		},
	}
	for k, v := range extraSpec {
		spec[k] = v
	}
	return domain.MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec":       spec,
	}
}

// getRayCluster fetches a RayCluster directly (bypassing the adapter) so a test
// can assert what was actually persisted, or that nothing was.
func getRayCluster(ctx context.Context, t *testing.T, k8s client.Client, namespace, name string) (*rayv1.RayCluster, error) {
	t.Helper()
	var rc rayv1.RayCluster
	err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rc)
	return &rc, err
}

// TestApplyDryRunPersistsNothing is the core dryRun AC: a DryRunAll apply
// validates the object but the resource MUST NOT exist afterward.
func TestApplyDryRunPersistsNothing(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "dryrun-cluster"
	spec := clusterMergedSpec(namespace, name, nil)

	out, err := adapter.Apply(ctx, domain.KindRayCluster, namespace, name, spec, true)
	if err != nil {
		t.Fatalf("Apply(dryRun): %v", err)
	}
	// The dry-run still returns a server-shaped object (validated view).
	if kind, _ := out["kind"].(string); kind != "RayCluster" {
		t.Errorf("dry-run read-back kind = %q, want RayCluster", kind)
	}

	// ...but nothing was persisted.
	if _, getErr := getRayCluster(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("after dry-run the cluster exists (get err = %v), want NotFound — dry-run must not mutate", getErr)
	}
}

// TestApplyCreatesAndReadsBack is the non-dryRun AC: a real apply persists the
// object and returns the server's read-back (with server-managed metadata
// populated, e.g. uid/resourceVersion), proving SSA round-tripped.
func TestApplyCreatesAndReadsBack(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "applied-cluster"
	spec := clusterMergedSpec(namespace, name, nil)

	out, err := adapter.Apply(ctx, domain.KindRayCluster, namespace, name, spec, false)
	if err != nil {
		t.Fatalf("Apply(commit): %v", err)
	}

	// Server-managed metadata is present in the read-back.
	meta, _ := out["metadata"].(map[string]any)
	if meta == nil || meta["uid"] == nil || meta["resourceVersion"] == nil {
		t.Errorf("read-back metadata lacks server fields (uid/resourceVersion): %v", meta)
	}

	// The object is actually persisted with our spec.
	rc, getErr := getRayCluster(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("after apply the cluster is not gettable: %v", getErr)
	}
	if rc.Spec.RayVersion != "2.9.0" {
		t.Errorf("persisted rayVersion = %q, want 2.9.0", rc.Spec.RayVersion)
	}
	if len(rc.Spec.WorkerGroupSpecs) != 1 || rc.Spec.WorkerGroupSpecs[0].GroupName != "workers" {
		t.Errorf("persisted worker groups = %+v, want one 'workers' group", rc.Spec.WorkerGroupSpecs)
	}
}

// TestApplyIsIdempotentUnderFieldManager applies the same spec twice with the
// ray-mcp field manager. SSA must treat the second apply as a no-op update (same
// owner, same fields) — not a conflict — proving the field-manager identity is
// stable and re-applies are safe.
func TestApplyIsIdempotentUnderFieldManager(t *testing.T) {
	adapter, _ := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "idempotent-cluster"
	spec := clusterMergedSpec(namespace, name, nil)

	if _, err := adapter.Apply(ctx, domain.KindRayCluster, namespace, name, spec, false); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := adapter.Apply(ctx, domain.KindRayCluster, namespace, name, spec, false); err != nil {
		t.Fatalf("second Apply (same manager, same fields) should be a clean no-op, got: %v", err)
	}
}

// TestApplyDryRunRejectsUnknownField is the validation-oracle AC, and records a
// domain-correctness fact for Task 9: under SERVER-SIDE APPLY, an unknown spec
// field is NOT silently pruned (the legacy create/update behavior the spec's Q4
// anticipated) — the apiserver REJECTS it with a hard `field not declared in
// schema` error. The unconditional DryRunAll surfaces that loudly, before any
// commit. The error is mapped through the domain taxonomy (not a raw dump).
func TestApplyDryRunRejectsUnknownField(t *testing.T) {
	adapter, _ := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "unknown-field-cluster"
	spec := clusterMergedSpec(namespace, name, map[string]any{
		"thisFieldDoesNotExistInTheCRD": "rejected-by-ssa",
	})

	_, err := adapter.Apply(ctx, domain.KindRayCluster, namespace, name, spec, true)
	if err == nil {
		t.Fatal("Apply(dryRun) with an unknown field returned nil error; SSA must reject it")
	}
	if !strings.Contains(err.Error(), "field not declared in schema") {
		t.Errorf("error %q does not name the unknown-field rejection", err.Error())
	}
}

// TestApplyDoesNotMutateCallerSpec is the real aliasing guard (the domain-layer
// test cannot reach it): client.Apply decodes the server read-back INTO the
// object it is given, and the adapter mutates GVK/name/namespace in place — so
// without the deep-copy the caller's intent map would be corrupted. The domain
// calls Apply twice with the same map, so this must hold. We snapshot the input,
// apply, and assert the input is byte-identical afterward.
func TestApplyDoesNotMutateCallerSpec(t *testing.T) {
	adapter, _ := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "no-mutate-cluster"
	spec := clusterMergedSpec(namespace, name, nil)
	snapshot := runtime.DeepCopyJSON(spec)

	if _, err := adapter.Apply(ctx, domain.KindRayCluster, namespace, name, spec, false); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	if !reflect.DeepEqual(map[string]any(spec), snapshot) {
		t.Errorf("Apply mutated the caller's spec map:\n got  %v\n want %v", spec, snapshot)
	}
}

// TestApplyServiceEndToEnd drives the DOMAIN ApplyService through the real
// envtest-backed adapter (the adapter satisfies domain.Applier). It proves the
// full pipeline end-to-end against a real apiserver: a dry-run that validates +
// persists nothing, then a commit that creates the object and is diffed against
// the read-back — with the audit choke point firing for each — without any fake.
func TestApplyServiceEndToEnd(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace, name = "default", "svc-applied"
	sink := &countingSink{}
	svc := domain.NewApplyService(adapter, sink)

	// A dry-run first: validates against the CRD schema, persists nothing.
	preview, err := svc.Apply(ctx, domain.ApplyRequest{
		Kind: domain.KindRayCluster, Namespace: namespace, Name: name,
		Spec:   clusterMergedSpec(namespace, name, nil),
		DryRun: true, Tool: "ray_cluster_create", ArgsSummary: "name=svc-applied",
	})
	if err != nil {
		t.Fatalf("preview Apply: %v", err)
	}
	if !preview.DryRun {
		t.Error("preview result DryRun = false, want true")
	}
	if _, getErr := getRayCluster(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("preview persisted the cluster (get err = %v), want NotFound", getErr)
	}

	// Now commit: the object is created and audit fired for both applies.
	committed, err := svc.Apply(ctx, domain.ApplyRequest{
		Kind: domain.KindRayCluster, Namespace: namespace, Name: name,
		Spec:   clusterMergedSpec(namespace, name, nil),
		DryRun: false, Tool: "ray_cluster_create", ArgsSummary: "name=svc-applied",
	})
	if err != nil {
		t.Fatalf("commit Apply: %v", err)
	}
	if committed.DryRun {
		t.Error("commit result DryRun = true, want false")
	}
	if committed.Object["metadata"] == nil {
		t.Error("commit result Object has no metadata, want the server read-back")
	}
	if _, getErr := getRayCluster(ctx, t, k8s, namespace, name); getErr != nil {
		t.Fatalf("commit did not persist the cluster: %v", getErr)
	}

	// Both applies emitted exactly one audit record each (the choke point).
	if sink.count != 2 {
		t.Errorf("audit records = %d, want 2 (one preview, one commit)", sink.count)
	}
}

// countingSink is a minimal domain.AuditSink that counts records, for the
// end-to-end audit-fires assertion.
type countingSink struct{ count int }

func (s *countingSink) Record(context.Context, domain.AuditRecord) { s.count++ }
