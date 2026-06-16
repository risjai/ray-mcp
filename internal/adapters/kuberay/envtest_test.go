//go:build envtest

// Tier 2 (envtest) coverage for the KubeRay adapter's read path. It boots a real
// kube-apiserver + etcd via controller-runtime's envtest, installs the KubeRay
// CRD bundle, and exercises the typed ListClusters/GetCluster implementation
// end-to-end: CR storage round-trip, status→DTO mapping, pagination via the k8s
// continue token, and the k8s→domain error taxonomy.
//
// There is NO KubeRay operator here — envtest is apiserver + etcd only — so
// .status is NEVER auto-populated. To test the status→DTO mapping
// deterministically the tests write .status themselves via the /status
// subresource (the v1.6.1 RayCluster CRD declares +kubebuilder:subresource:status,
// which envtest serves).
//
// Run via `make test-envtest`, which fetches the CRDs and resolves
// KUBEBUILDER_ASSETS for the pinned K8s version.
package kuberay

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/risjai/ray-mcp/internal/domain"
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

// startAdapter boots envtest with the KubeRay CRDs and returns the real adapter
// (backed by an uncached controller-runtime client whose scheme carries the
// typed KubeRay v1 types) plus the raw client for status-subresource writes the
// adapter's read API does not expose. Cleanup stops the environment.
func startAdapter(t *testing.T) (*Client, client.Client) {
	t.Helper()

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

	scheme, err := newScheme()
	if err != nil {
		t.Fatalf("build adapter scheme: %v", err)
	}

	k8s, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("build controller-runtime client: %v", err)
	}

	return newRuntimeClient("envtest", "default", k8s), k8s
}

// newRayCluster builds a minimal valid RayCluster (head group + one worker
// group). envtest validates the spec against the CRD schema on create, so the
// spec must satisfy the required fields.
func newRayCluster(namespace, name string) *rayv1.RayCluster {
	headContainer := corev1.Container{Name: "ray-head", Image: "rayproject/ray:2.9.0"}
	workerContainer := corev1.Container{Name: "ray-worker", Image: "rayproject/ray:2.9.0"}

	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{headContainer}},
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
				GroupName:      "workers",
				Replicas:       ptr[int32](2),
				MinReplicas:    ptr[int32](0),
				MaxReplicas:    ptr[int32](5),
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{Containers: []corev1.Container{workerContainer}},
				},
			}},
		},
	}
}

func ptr[T any](v T) *T { return &v }

// TestGetClusterMapsStatus creates a RayCluster, writes a Ready status via the
// /status subresource (no operator runs in envtest), and asserts GetCluster maps
// the typed status into the domain ClusterDetail.
func TestGetClusterMapsStatus(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "ready-cluster"
		headSvc   = "ready-cluster-head-svc"
	)

	rc := newRayCluster(namespace, name)
	if err := k8s.Create(ctx, rc); err != nil {
		t.Fatalf("create RayCluster: %v", err)
	}

	// envtest runs no operator, so populate .status ourselves via the status
	// subresource to drive the mapping deterministically.
	rc.Status = rayv1.RayClusterStatus{
		State:                 rayv1.Ready,
		ReadyWorkerReplicas:   2,
		DesiredWorkerReplicas: 2,
		Head:                  rayv1.HeadInfo{ServiceName: headSvc, PodIP: "10.0.0.1"},
		Conditions: []metav1.Condition{
			cond(string(rayv1.RayClusterProvisioned), metav1.ConditionTrue, "Provisioned", "all pods created"),
			cond(string(rayv1.HeadPodReady), metav1.ConditionTrue, "HeadReady", "head pod ready"),
		},
	}
	if err := k8s.Status().Update(ctx, rc); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	detail, err := adapter.GetCluster(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}

	if detail.Name != name {
		t.Errorf("Name = %q, want %q", detail.Name, name)
	}
	if detail.Namespace != namespace {
		t.Errorf("Namespace = %q, want %q", detail.Namespace, namespace)
	}
	if detail.Phase != "Ready" {
		t.Errorf("Phase = %q, want %q", detail.Phase, "Ready")
	}
	if detail.ReadyReplicas != 2 {
		t.Errorf("ReadyReplicas = %d, want 2", detail.ReadyReplicas)
	}
	if detail.DesiredReplicas != 2 {
		t.Errorf("DesiredReplicas = %d, want 2", detail.DesiredReplicas)
	}
	if detail.HeadServiceName != headSvc {
		t.Errorf("HeadServiceName = %q, want %q", detail.HeadServiceName, headSvc)
	}
	wantURL := "http://" + headSvc + ".default.svc:8265"
	if detail.DashboardURL != wantURL {
		t.Errorf("DashboardURL = %q, want %q", detail.DashboardURL, wantURL)
	}
	if detail.Age <= 0 {
		t.Errorf("Age = %v, want > 0", detail.Age)
	}
	if detail.Raw == nil {
		t.Error("Raw is nil, want the full object map")
	} else if kind, _ := detail.Raw["kind"].(string); kind != "RayCluster" {
		t.Errorf("Raw[kind] = %q, want RayCluster", kind)
	}
}

// TestGetClusterNoHeadServiceNoDashboardURL asserts that with no head service
// name in status the synthesized dashboard URL is empty (never a fabricated
// guess) and the phase reflects a still-provisioning cluster.
func TestGetClusterNoHeadServiceNoDashboardURL(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "provisioning-cluster"
	)

	rc := newRayCluster(namespace, name)
	if err := k8s.Create(ctx, rc); err != nil {
		t.Fatalf("create RayCluster: %v", err)
	}

	rc.Status = rayv1.RayClusterStatus{
		ReadyWorkerReplicas:   0,
		DesiredWorkerReplicas: 2,
		Conditions: []metav1.Condition{
			cond(string(rayv1.RayClusterProvisioned), metav1.ConditionFalse, "Provisioning", "waiting for pods"),
		},
	}
	if err := k8s.Status().Update(ctx, rc); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	detail, err := adapter.GetCluster(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetCluster: %v", err)
	}

	if detail.Phase != "Provisioning" {
		t.Errorf("Phase = %q, want %q", detail.Phase, "Provisioning")
	}
	if detail.HeadServiceName != "" {
		t.Errorf("HeadServiceName = %q, want empty", detail.HeadServiceName)
	}
	if detail.DashboardURL != "" {
		t.Errorf("DashboardURL = %q, want empty (no fabricated guess)", detail.DashboardURL)
	}
}

// TestGetClusterNotFound asserts a missing name maps to *domain.NotFoundError.
func TestGetClusterNotFound(t *testing.T) {
	adapter, _ := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := adapter.GetCluster(ctx, "default", "does-not-exist")
	if err == nil {
		t.Fatal("GetCluster on a missing name returned nil error, want NotFoundError")
	}

	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *domain.NotFoundError", err, err)
	}
	if nf.Kind != domain.KindRayCluster {
		t.Errorf("NotFoundError.Kind = %q, want %q", nf.Kind, domain.KindRayCluster)
	}
	if nf.Name != "does-not-exist" {
		t.Errorf("NotFoundError.Name = %q, want %q", nf.Name, "does-not-exist")
	}
	if nf.Namespace != "default" {
		t.Errorf("NotFoundError.Namespace = %q, want %q", nf.Namespace, "default")
	}
}

// TestListClustersRoundTrips asserts a created cluster shows up in the list with
// its mapped summary fields.
func TestListClustersRoundTrips(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "list-one"
	)

	rc := newRayCluster(namespace, name)
	if err := k8s.Create(ctx, rc); err != nil {
		t.Fatalf("create RayCluster: %v", err)
	}
	rc.Status = rayv1.RayClusterStatus{
		State:                 rayv1.Ready,
		ReadyWorkerReplicas:   2,
		DesiredWorkerReplicas: 2,
		Conditions: []metav1.Condition{
			cond(string(rayv1.RayClusterProvisioned), metav1.ConditionTrue, "Provisioned", ""),
			cond(string(rayv1.HeadPodReady), metav1.ConditionTrue, "HeadReady", ""),
		},
	}
	if err := k8s.Status().Update(ctx, rc); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	list, err := adapter.ListClusters(ctx, namespace, domain.ListOptions{})
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}

	got := findSummary(list.Items, name)
	if got == nil {
		t.Fatalf("ListClusters did not return %q; items = %+v", name, list.Items)
	}
	if got.Phase != "Ready" {
		t.Errorf("summary Phase = %q, want %q", got.Phase, "Ready")
	}
	if got.ReadyReplicas != 2 || got.DesiredReplicas != 2 {
		t.Errorf("summary ready/desired = %d/%d, want 2/2", got.ReadyReplicas, got.DesiredReplicas)
	}
}

// TestListClustersPagination creates three clusters, lists with Limit=1, and
// follows the continue token, asserting the k8s token surfaces verbatim and
// paging walks the full set without silent truncation.
func TestListClustersPagination(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace = "default"
	names := []string{"page-a", "page-b", "page-c"}
	for _, name := range names {
		if err := k8s.Create(ctx, newRayCluster(namespace, name)); err != nil {
			t.Fatalf("create %q: %v", name, err)
		}
	}

	seen := map[string]bool{}
	cont := ""
	pages := 0
	for {
		list, err := adapter.ListClusters(ctx, namespace, domain.ListOptions{Limit: 1, Continue: cont})
		if err != nil {
			t.Fatalf("ListClusters (continue=%q): %v", cont, err)
		}
		if len(list.Items) != 1 {
			t.Fatalf("page %d returned %d items, want exactly 1 (Limit=1)", pages, len(list.Items))
		}
		seen[list.Items[0].Name] = true
		pages++

		if list.Continue == "" {
			break
		}
		cont = list.Continue

		if pages > len(names)+1 {
			t.Fatalf("pagination did not terminate after %d pages", pages)
		}
	}

	if pages != len(names) {
		t.Errorf("walked %d pages, want %d (one per cluster)", pages, len(names))
	}
	for _, name := range names {
		if !seen[name] {
			t.Errorf("cluster %q never appeared across paged listing", name)
		}
	}
}

// TestClusterServiceEndToEnd drives the DOMAIN ClusterService through the real
// envtest-backed adapter (the adapter satisfies domain.ClusterReader). It proves
// the full read path end-to-end: namespace defaulting, the distilled-vs-verbose
// Raw gate, the "more available vs showing all" continue signal, and
// NotFound propagation — all against a real apiserver, not a fake port.
func TestClusterServiceEndToEnd(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace = "default"
	names := []string{"svc-a", "svc-b", "svc-c"}
	for _, name := range names {
		if err := k8s.Create(ctx, newRayCluster(namespace, name)); err != nil {
			t.Fatalf("create %q: %v", name, err)
		}
	}

	svc := domain.NewClusterService(adapter, namespace)

	// List with the default namespace (empty request namespace) returns the
	// seeded clusters; with no Limit there is no continue token, so the honest
	// signal is "showing all".
	all, err := svc.List(ctx, domain.ClusterListRequest{})
	if err != nil {
		t.Fatalf("List (default namespace): %v", err)
	}
	if len(all.Items) != len(names) {
		t.Fatalf("List returned %d clusters, want %d", len(all.Items), len(names))
	}
	if all.MoreAvailable {
		t.Errorf("MoreAvailable = true with no page cap, want false (showing all)")
	}

	// Limit=1 forces pagination: the first page surfaces a continue token, so the
	// signal flips to "more available".
	page, err := svc.List(ctx, domain.ClusterListRequest{Limit: 1})
	if err != nil {
		t.Fatalf("List (Limit=1): %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("List(Limit=1) returned %d items, want 1", len(page.Items))
	}
	if !page.MoreAvailable || page.Continue == "" {
		t.Errorf("List(Limit=1) MoreAvailable=%v Continue=%q, want true + a token", page.MoreAvailable, page.Continue)
	}

	// Get distilled by default: Raw must be nil.
	distilled, err := svc.Get(ctx, domain.ClusterGetRequest{Name: "svc-a"})
	if err != nil {
		t.Fatalf("Get (distilled): %v", err)
	}
	if distilled.Detail.Raw != nil {
		t.Errorf("distilled Get Raw = %+v, want nil", distilled.Detail.Raw)
	}
	if distilled.Detail.Name != "svc-a" {
		t.Errorf("distilled Get Name = %q, want svc-a", distilled.Detail.Name)
	}

	// Get verbose: Raw must carry the full object.
	verbose, err := svc.Get(ctx, domain.ClusterGetRequest{Name: "svc-a", Verbose: true})
	if err != nil {
		t.Fatalf("Get (verbose): %v", err)
	}
	if verbose.Detail.Raw == nil {
		t.Fatalf("verbose Get Raw = nil, want the full object")
	}
	if kind, _ := verbose.Detail.Raw["kind"].(string); kind != "RayCluster" {
		t.Errorf("verbose Get Raw[kind] = %q, want RayCluster", kind)
	}

	// NotFound propagates through the service unchanged.
	_, err = svc.Get(ctx, domain.ClusterGetRequest{Name: "does-not-exist"})
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get(missing) error = %T (%v), want *domain.NotFoundError", err, err)
	}
}

// cond builds a metav1.Condition with the fields the mapping reads. LastTransitionTime
// is required by the apiserver when writing conditions.
func cond(condType string, status metav1.ConditionStatus, reason, message string) metav1.Condition {
	return metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: metav1.Now(),
	}
}

// findSummary returns the named summary from a list page, or nil.
func findSummary(items []domain.ClusterSummary, name string) *domain.ClusterSummary {
	for i := range items {
		if items[i].Name == name {
			return &items[i]
		}
	}
	return nil
}
