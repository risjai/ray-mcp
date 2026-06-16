//go:build envtest

// Tier 2 (envtest) smoke test for the KubeRay adapter substrate. It boots a real
// kube-apiserver + etcd via controller-runtime's envtest, installs the KubeRay
// CRD bundle, and proves a RayCluster custom resource round-trips through CR
// storage. There is NO KubeRay operator and NO Ray pods here — envtest is
// apiserver + etcd only, so this tier proves CR storage, not reconciliation
// (that is tier 5 / e2e).
//
// Deliberately uses unstructured.Unstructured (apiVersion ray.io/v1, kind
// RayCluster) rather than the typed ray-operator/apis/ray/v1 package: Task 4.5
// proves the harness/substrate, and the typed client lands with the real adapter
// in Task 5. Keeping unstructured here keeps that dependency one task out and the
// hexagonal seam clean.
//
// Run via `make test-envtest`, which fetches the CRDs and resolves
// KUBEBUILDER_ASSETS for the pinned K8s version.
package kuberay

import (
	"context"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// crdDir resolves the gitignored CRD bundle directory relative to this test
// file. envtest tests run with CWD = the package dir, so deriving the path from
// the source file location (rather than CWD) is robust regardless of how the
// test is invoked.
func crdDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed: cannot locate test source file")
	}
	// thisFile = <repo>/internal/adapters/kuberay/envtest_test.go
	// CRDs live at <repo>/test/crds.
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..", "..")
	return filepath.Join(repoRoot, "test", "crds")
}

func TestEnvtestRayClusterRoundTrips(t *testing.T) {
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir(t)},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest start: %v (did `make envtest-crds` run and KUBEBUILDER_ASSETS resolve? use `make test-envtest`)", err)
	}
	t.Cleanup(func() {
		if stopErr := env.Stop(); stopErr != nil {
			t.Errorf("envtest stop: %v", stopErr)
		}
	})

	c, err := client.New(cfg, client.Options{})
	if err != nil {
		t.Fatalf("build controller-runtime client: %v", err)
	}

	const (
		namespace = "default"
		name      = "smoke-cluster"
	)
	gvk := schema.GroupVersionKind{Group: "ray.io", Version: "v1", Kind: "RayCluster"}

	// A minimal RayCluster CR built as unstructured — no typed KubeRay apis.
	rc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ray.io/v1",
		"kind":       "RayCluster",
		"metadata": map[string]any{
			"name":      name,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"headGroupSpec": map[string]any{
				"rayStartParams": map[string]any{},
				"template": map[string]any{
					"spec": map[string]any{
						"containers": []any{
							map[string]any{
								"name":  "ray-head",
								"image": "rayproject/ray:2.9.0",
							},
						},
					},
				},
			},
		},
	}}
	rc.SetGroupVersionKind(gvk)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create proves the CRD registered (the apiserver accepts the GVK) and CR
	// storage works (etcd persists it).
	if err := c.Create(ctx, rc); err != nil {
		t.Fatalf("create RayCluster CR (CRD not registered or storage broken): %v", err)
	}

	// Get it back into a fresh unstructured and assert the object round-trips.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(gvk)
	if err := c.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, got); err != nil {
		t.Fatalf("get RayCluster CR back: %v", err)
	}

	if got.GetName() != name {
		t.Errorf("round-trip name = %q, want %q", got.GetName(), name)
	}
	if got.GetNamespace() != namespace {
		t.Errorf("round-trip namespace = %q, want %q", got.GetNamespace(), namespace)
	}
	if gotGVK := got.GroupVersionKind(); gotGVK != gvk {
		t.Errorf("round-trip GVK = %v, want %v", gotGVK, gvk)
	}
}
