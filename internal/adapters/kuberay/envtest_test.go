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

// TestClusterEventsGathersAndFilters is the Task 7 end-to-end proof. envtest
// runs no operator and no kubelet/scheduler, so NO events appear on their own —
// the test CREATES core/v1 events itself to drive the adapter's gather/merge/
// filter/sort/bound mapping. It proves, empirically:
//   - newScheme() registers core/v1 (Pods + Events decode);
//   - pod resolution by the ray.io/cluster=<name> label works;
//   - events on BOTH the RayCluster object and its labeled pods are gathered;
//   - an event on a pod NOT carrying the label is excluded;
//   - the result is sorted Warnings-first then recent, with Count/LastSeen mapped.
func TestClusterEventsGathersAndFilters(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "events-cluster"
	)

	rc := newRayCluster(namespace, name)
	if err := k8s.Create(ctx, rc); err != nil {
		t.Fatalf("create RayCluster: %v", err)
	}

	// Two pods labeled for this cluster (head + worker) and one NOT labeled, to
	// prove the label selector scopes the gather.
	headPod := newPod(namespace, "events-cluster-head", map[string]string{"ray.io/cluster": name})
	workerPod := newPod(namespace, "events-cluster-worker", map[string]string{"ray.io/cluster": name})
	otherPod := newPod(namespace, "unrelated-pod", map[string]string{"ray.io/cluster": "some-other-cluster"})
	for _, p := range []*corev1.Pod{headPod, workerPod, otherPod} {
		if err := k8s.Create(ctx, p); err != nil {
			t.Fatalf("create pod %q: %v", p.Name, err)
		}
	}

	now := time.Now()
	// Event on the RayCluster object itself (operator/reconcile event).
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "ev-rc", "RayCluster", name,
		corev1.EventTypeWarning, "FailedToCreateHeadPod", "head pod create failed", 1, now.Add(-30*time.Second)))
	// Warning on a labeled pod (the "no GPU nodes" signal).
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "ev-sched", "Pod", workerPod.Name,
		corev1.EventTypeWarning, "FailedScheduling", "0/1 nodes: insufficient nvidia.com/gpu", 5, now.Add(-10*time.Second)))
	// Normal on a labeled pod (most recent, but Normal — must sort after Warnings).
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "ev-pulled", "Pod", headPod.Name,
		corev1.EventTypeNormal, "Pulled", "image pulled", 1, now))
	// Event on the UNLABELED pod — must be excluded.
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "ev-other", "Pod", otherPod.Name,
		corev1.EventTypeWarning, "FailedScheduling", "should not appear", 1, now))

	events, err := adapter.Events(ctx, domain.KindRayCluster, namespace, name, 25)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}

	if len(events) != 3 {
		t.Fatalf("Events returned %d events, want 3 (cluster + 2 labeled-pod events, unlabeled excluded); got %+v", len(events), events)
	}

	byReason := map[string]domain.Event{}
	for _, e := range events {
		byReason[e.Reason+"/"+e.Message] = e
	}
	if _, ok := byReason["should not appear"]; ok {
		t.Error("event on the UNLABELED pod was NOT excluded")
	}

	// The two Warnings must sort before the Normal regardless of recency (the
	// Normal Pulled event is the most recent yet must come last).
	if events[0].Type != corev1.EventTypeWarning || events[1].Type != corev1.EventTypeWarning {
		t.Errorf("first two events = %q,%q, want both Warning (Warnings prioritized)", events[0].Type, events[1].Type)
	}
	if events[2].Type != corev1.EventTypeNormal || events[2].Reason != "Pulled" {
		t.Errorf("last event = %q/%q, want the Normal Pulled event (Normals trimmed/sorted last)", events[2].Type, events[2].Reason)
	}
	// Among the Warnings, the more recent FailedScheduling must precede the older
	// FailedToCreateHeadPod.
	if events[0].Reason != "FailedScheduling" {
		t.Errorf("events[0].Reason = %q, want FailedScheduling (most recent Warning first)", events[0].Reason)
	}

	// Count + LastSeen mapping: FailedScheduling carried count=5.
	sched := events[0]
	if sched.Count != 5 {
		t.Errorf("FailedScheduling Count = %d, want 5", sched.Count)
	}
	if sched.LastSeen.IsZero() {
		t.Error("FailedScheduling LastSeen is zero, want the mapped lastTimestamp")
	}
}

// TestClusterEventsRespectsLimitAndWarningPriority proves the bounding keeps
// Warnings when trimming: with more events than the limit, the kept set is the
// Warnings (the actionable tier), Normals dropped first.
func TestClusterEventsRespectsLimitAndWarningPriority(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "bounded-cluster"
	)

	if err := k8s.Create(ctx, newRayCluster(namespace, name)); err != nil {
		t.Fatalf("create RayCluster: %v", err)
	}
	pod := newPod(namespace, "bounded-pod", map[string]string{"ray.io/cluster": name})
	if err := k8s.Create(ctx, pod); err != nil {
		t.Fatalf("create pod: %v", err)
	}

	now := time.Now()
	// One Warning + two more-recent Normals on the labeled pod.
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "b-warn", "Pod", pod.Name,
		corev1.EventTypeWarning, "BackOff", "back-off restarting", 2, now.Add(-time.Minute)))
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "b-n1", "Pod", pod.Name,
		corev1.EventTypeNormal, "Pulled", "pulled a", 1, now))
	mustCreateEvent(ctx, t, k8s, fakeEvent(namespace, "b-n2", "Pod", pod.Name,
		corev1.EventTypeNormal, "Created", "created b", 1, now.Add(-time.Second)))

	events, err := adapter.Events(ctx, domain.KindRayCluster, namespace, name, 1)
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("Events with limit=1 returned %d, want 1", len(events))
	}
	if events[0].Type != corev1.EventTypeWarning || events[0].Reason != "BackOff" {
		t.Errorf("kept event = %q/%q, want the Warning BackOff (Warnings survive the trim over more-recent Normals)", events[0].Type, events[0].Reason)
	}
}

// newPod builds a minimal pod for the event-resolution tests.
func newPod(namespace, name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "main", Image: "rayproject/ray:2.9.0"}},
		},
	}
}

// fakeEvent builds a core/v1 Event referencing the given involvedObject. The
// apiserver requires metadata.name on a namespaced event; lastTimestamp/count/
// type drive the adapter's mapping.
func fakeEvent(namespace, name, involvedKind, involvedName, eventType, reason, message string, count int32, last time.Time) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		InvolvedObject: corev1.ObjectReference{
			Kind:      involvedKind,
			Namespace: namespace,
			Name:      involvedName,
		},
		Type:           eventType,
		Reason:         reason,
		Message:        message,
		Count:          count,
		FirstTimestamp: metav1.NewTime(last),
		LastTimestamp:  metav1.NewTime(last),
	}
}

// mustCreateEvent creates an event or fails the test.
func mustCreateEvent(ctx context.Context, t *testing.T, k8s client.Client, ev *corev1.Event) {
	t.Helper()
	if err := k8s.Create(ctx, ev); err != nil {
		t.Fatalf("create event %q: %v", ev.Name, err)
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
