// Tier 1 (unit) coverage for the reachability adapter. These run in the fast
// no-tags loop with a fake dialer + fake clock + controller-runtime fake client,
// so they prove strategy selection, head resolution, pooling, idle reaping and
// re-dial-once with NO real cluster and NO real time (plan.md Task 14).
package reachability

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// --- test seams -------------------------------------------------------------

// fakeHandle is a TunnelHandle with no real SPDY stream: a fixed local port and a
// lost channel the test closes to simulate a dropped tunnel.
type fakeHandle struct {
	localPort int
	lost      chan struct{}

	mu     sync.Mutex
	closed bool
}

func newFakeHandle(port int) *fakeHandle {
	return &fakeHandle{localPort: port, lost: make(chan struct{})}
}

func (h *fakeHandle) LocalPort() int        { return h.localPort }
func (h *fakeHandle) Lost() <-chan struct{} { return h.lost }
func (h *fakeHandle) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
}

func (h *fakeHandle) isClosed() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}

// drop simulates the tunnel's connection dying (head pod rescheduled, SPDY stream
// errored): the next pool lookup sees Lost() closed and re-dials once.
func (h *fakeHandle) drop() { close(h.lost) }

// fakeDialer records every Dial and hands out incrementing local ports so each
// tunnel has a distinct base URL. dialErr, when set, fails the Nth dial onward to
// prove the re-dial-once-then-degrade contract.
type fakeDialer struct {
	mu       sync.Mutex
	calls    []dialCall
	handles  []*fakeHandle
	nextPort int
	failFrom int // 0 = never fail; otherwise fail the failFrom-th dial and later.
}

type dialCall struct {
	namespace  string
	podName    string
	remotePort int
}

func (d *fakeDialer) Dial(_ context.Context, namespace, podName string, remotePort int) (TunnelHandle, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, dialCall{namespace, podName, remotePort})
	if d.failFrom != 0 && len(d.calls) >= d.failFrom {
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: "dial failed"}
	}
	if d.nextPort == 0 {
		d.nextPort = 30000
	}
	d.nextPort++
	h := newFakeHandle(d.nextPort)
	d.handles = append(d.handles, h)
	return h, nil
}

func (d *fakeDialer) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.calls)
}

// fakeClock is a manually-advanced clock so idle-reaping is deterministic.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// --- fixtures ---------------------------------------------------------------

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := rayv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add rayv1 to scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 to scheme: %v", err)
	}
	return scheme
}

// resolverFor builds a Resolver over a fake k8s client seeded with the given
// objects and a fake dialer, with the in-cluster probe forced to inCluster.
func resolverFor(t *testing.T, rayAccess string, inCluster bool, dialer TunnelDialer, objs ...client.Object) *Resolver {
	t.Helper()
	k8s := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(objs...). // WithObjects so Status survives Get on the fake client.
		Build()

	r := NewResolver(&config.Config{RayAccess: rayAccess}, k8s, dialer)
	r.inCluster = func() bool { return inCluster }
	// Drive reaping manually in tests; never start the production ticker goroutine.
	r.reaperOnce.Do(func() {})
	return r
}

func clusterWithHeadService(ns, name, svc string) *rayv1.RayCluster {
	rc := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}}
	rc.Status.Head.ServiceName = svc
	return rc
}

func rayPod(ns, name, cluster, nodeType string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels:    map[string]string{clusterLabel: cluster, nodeTypeLabel: nodeType},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// --- AC#1: strategy selection ----------------------------------------------

func TestSelectionAutoInClusterUsesDirectDial(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "auto", true, dialer, clusterWithHeadService("ray", "demo", "demo-head-svc"))

	ep, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if want := "http://demo-head-svc.ray.svc:8265"; ep.BaseURL != want {
		t.Errorf("BaseURL = %q, want in-cluster service URL %q", ep.BaseURL, want)
	}
	if dialer.callCount() != 0 {
		t.Errorf("DirectDial must not port-forward: Dial called %d times", dialer.callCount())
	}
}

func TestSelectionAutoOutOfClusterUsesPortForward(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "auto", false, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))

	ep, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if want := fmt.Sprintf("http://127.0.0.1:%d", dialer.handles[0].LocalPort()); ep.BaseURL != want {
		t.Errorf("BaseURL = %q, want local tunnel URL %q", ep.BaseURL, want)
	}
	if dialer.callCount() != 1 {
		t.Errorf("PortForward must dial once: Dial called %d times", dialer.callCount())
	}
}

func TestSelectionDirectOverrideHonoredOutOfCluster(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	// --ray-access=direct out-of-cluster: override short-circuits detection.
	r := resolverFor(t, "direct", false, dialer, clusterWithHeadService("ray", "demo", "demo-head-svc"))

	ep, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if want := "http://demo-head-svc.ray.svc:8265"; ep.BaseURL != want {
		t.Errorf("BaseURL = %q, want service URL %q (override honored)", ep.BaseURL, want)
	}
	if dialer.callCount() != 0 {
		t.Errorf("direct override must not port-forward: Dial called %d times", dialer.callCount())
	}
}

func TestSelectionPortForwardOverrideHonoredInCluster(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	// --ray-access=port-forward in-cluster: override short-circuits detection.
	r := resolverFor(t, "port-forward", true, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))

	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if dialer.callCount() != 1 {
		t.Errorf("port-forward override must dial: Dial called %d times", dialer.callCount())
	}
}

func TestDirectDialUnprovisionedHeadServiceDegrades(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	// RayCluster exists but Status.Head.ServiceName is empty (head not yet up).
	r := resolverFor(t, "direct", true, dialer, clusterWithHeadService("ray", "demo", ""))

	_, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	var unreachable *domain.RayAPIUnreachableError
	if !errors.As(err, &unreachable) {
		t.Fatalf("err = %v, want RayAPIUnreachableError for an unprovisioned head service", err)
	}
}

func TestDefaultPortAppliedWhenZero(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "direct", true, dialer, clusterWithHeadService("ray", "demo", "demo-head-svc"))

	ep, err := r.Endpoint(context.Background(), "ray", "demo", 0)
	if err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if want := "http://demo-head-svc.ray.svc:8265"; ep.BaseURL != want {
		t.Errorf("BaseURL = %q, want default port 8265 applied %q", ep.BaseURL, want)
	}
}

// TestHeadPodSelectorIsolatesHead proves the load-bearing C2 selector: with a
// head AND a worker pod under the same cluster, headPodName returns the head.
func TestHeadPodSelectorIsolatesHead(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-worker-1", "demo", "worker", corev1.PodRunning),
		rayPod("ray", "demo-head-xyz", "demo", headNodeType, corev1.PodRunning),
	)

	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if got := dialer.calls[0].podName; got != "demo-head-xyz" {
		t.Errorf("dialed pod %q, want the head pod (selector must exclude workers)", got)
	}
}

// TestHeadPodSelectorSkipsTerminatingHead proves that during a head reschedule a
// Running-but-terminating pod is skipped in favor of the fresh replacement.
func TestHeadPodSelectorSkipsTerminatingHead(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	// The terminating pod is named to sort BEFORE the fresh one (the fake client
	// returns the list name-sorted), so without the DeletionTimestamp guard it
	// would be the one picked — making this test fail if the guard regresses.
	terminating := rayPod("ray", "demo-head-aaa", "demo", headNodeType, corev1.PodRunning)
	now := metav1.NewTime(time.Unix(1_700_000_000, 0))
	terminating.DeletionTimestamp = &now
	terminating.Finalizers = []string{"ray.io/finalizer"} // fake client requires a finalizer to retain a deletion-stamped object.
	fresh := rayPod("ray", "demo-head-zzz", "demo", headNodeType, corev1.PodRunning)

	r := resolverFor(t, "port-forward", false, dialer, terminating, fresh)

	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	if got := dialer.calls[0].podName; got != "demo-head-zzz" {
		t.Errorf("dialed pod %q, want the non-terminating head %q", got, "demo-head-zzz")
	}
}

func TestHeadPodSelectorNoRunningHeadDegrades(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	// Only a worker is running; no running head pod.
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-worker-1", "demo", "worker", corev1.PodRunning))

	_, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	var unreachable *domain.RayAPIUnreachableError
	if !errors.As(err, &unreachable) {
		t.Fatalf("err = %v, want RayAPIUnreachableError when no running head pod", err)
	}
	if dialer.callCount() != 0 {
		t.Errorf("must not dial when head pod resolution fails: Dial called %d times", dialer.callCount())
	}
}

// --- AC#2: pooling, reaping, re-dial-once -----------------------------------

func TestPoolingReusesWarmTunnel(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))

	first, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("first Endpoint: %v", err)
	}
	second, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("second Endpoint: %v", err)
	}

	if first.BaseURL != second.BaseURL {
		t.Errorf("warm reuse must return the same URL: %q then %q", first.BaseURL, second.BaseURL)
	}
	if dialer.callCount() != 1 {
		t.Errorf("warm reuse must not re-dial: Dial called %d times, want 1", dialer.callCount())
	}
}

func TestPoolingKeysPerCluster(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "a-head", "a", headNodeType, corev1.PodRunning),
		rayPod("ray", "b-head", "b", headNodeType, corev1.PodRunning))

	a, _ := r.Endpoint(context.Background(), "ray", "a", 8265)
	b, _ := r.Endpoint(context.Background(), "ray", "b", 8265)

	if a.BaseURL == b.BaseURL {
		t.Errorf("distinct clusters must get distinct tunnels: both %q", a.BaseURL)
	}
	if dialer.callCount() != 2 {
		t.Errorf("two clusters must dial twice: Dial called %d times", dialer.callCount())
	}
}

func TestReaperClosesIdleTunnel(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))
	r.pool.now = clock.Now

	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("Endpoint: %v", err)
	}
	handle := dialer.handles[0]

	clock.advance(defaultIdleTimeout + time.Second)
	if closed := r.pool.reap(clock.Now()); closed != 1 {
		t.Errorf("reap closed %d tunnels, want 1", closed)
	}
	if !handle.isClosed() {
		t.Error("reaped tunnel handle was not Closed")
	}

	// A subsequent call must re-dial (the pooled tunnel is gone).
	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("Endpoint after reap: %v", err)
	}
	if dialer.callCount() != 2 {
		t.Errorf("call after reap must re-dial: Dial called %d times, want 2", dialer.callCount())
	}
}

func TestReaperLeavesFreshTunnel(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))
	r.pool.now = clock.Now

	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("Endpoint: %v", err)
	}

	clock.advance(defaultIdleTimeout - time.Second) // still within the idle window.
	if closed := r.pool.reap(clock.Now()); closed != 0 {
		t.Errorf("reap closed %d tunnels, want 0 (still fresh)", closed)
	}
	if dialer.callCount() != 1 {
		t.Errorf("fresh tunnel must not be re-dialed: Dial called %d times", dialer.callCount())
	}
}

func TestDroppedTunnelRedialsOnce(t *testing.T) {
	t.Parallel()
	dialer := &fakeDialer{}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))

	first, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("first Endpoint: %v", err)
	}
	stale := dialer.handles[0]
	stale.drop() // simulate the tunnel dropping (head pod rescheduled).

	second, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("Endpoint after drop: %v", err)
	}

	if !stale.isClosed() {
		t.Error("dropped tunnel handle was not torn down before re-dial")
	}
	if dialer.callCount() != 2 {
		t.Errorf("dropped tunnel must re-dial exactly once: Dial called %d times, want 2", dialer.callCount())
	}
	if first.BaseURL == second.BaseURL {
		t.Errorf("re-dial must yield a fresh tunnel URL: both %q", first.BaseURL)
	}
}

func TestRedialFailureDegradesNoRetryLoop(t *testing.T) {
	t.Parallel()
	// First dial succeeds, the re-dial (2nd) fails: prove "once then degrade",
	// not an infinite retry.
	dialer := &fakeDialer{failFrom: 2}
	r := resolverFor(t, "port-forward", false, dialer,
		rayPod("ray", "demo-head-abc", "demo", headNodeType, corev1.PodRunning))

	if _, err := r.Endpoint(context.Background(), "ray", "demo", 8265); err != nil {
		t.Fatalf("first Endpoint: %v", err)
	}
	dialer.handles[0].drop()

	_, err := r.Endpoint(context.Background(), "ray", "demo", 8265)
	var unreachable *domain.RayAPIUnreachableError
	if !errors.As(err, &unreachable) {
		t.Fatalf("err = %v, want RayAPIUnreachableError when re-dial fails", err)
	}
	if dialer.callCount() != 2 {
		t.Errorf("re-dial must happen exactly once then degrade: Dial called %d times, want 2", dialer.callCount())
	}
}
