package reachability

import (
	"context"
	"fmt"
	"sync"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// Resolver implements domain.RayReachability. It resolves a usable base URL for a
// cluster's head dashboard via one of two strategies (spec §5, Q6): DirectDial
// (an in-cluster service DNS URL, no port-forward RBAC) or pooled SPDY
// PortForward (out-of-cluster). The strategy is chosen from cfg.RayAccess —
// "direct"/"port-forward" force a mode, "auto" picks DirectDial in-cluster and
// PortForward otherwise.
//
// Like the kuberay adapter, NewResolver touches no network; the in-cluster probe
// and any dial happen lazily on the first Endpoint call. k8s is the uncached
// controller-runtime reader used to resolve the head service (DirectDial) and
// head pod (PortForward); its scheme must have rayv1 + corev1 registered.
type Resolver struct {
	cfg  *config.Config
	k8s  client.Client
	pool *pool

	// inCluster reports whether the process runs inside a Kubernetes pod. It is a
	// field, not a direct rest.InClusterConfig() call, so unit tests can force the
	// selection branch without manipulating the environment or SA token files.
	inCluster func() bool

	reaperOnce sync.Once
}

// compile-time check that the adapter satisfies the domain port.
var _ domain.RayReachability = (*Resolver)(nil)

// defaultDashboardPort is the Ray head dashboard / Job Submission REST API port,
// used when Endpoint is called with port <= 0 (matching kuberay/cluster.go's 8265).
const defaultDashboardPort = 8265

// defaultIdleTimeout is how long a pooled port-forward tunnel may sit unused
// before the reaper closes it. Tunnels are a process-local cache (spec §4), so a
// few idle minutes balances reuse against holding an SPDY stream open needlessly.
const defaultIdleTimeout = 5 * time.Minute

// accessMode is the resolved reachability strategy.
type accessMode int

const (
	modeDirect accessMode = iota
	modePortForward
)

// NewResolver builds the reachability adapter WITHOUT touching the network (the
// boot invariant the kuberay adapter also keeps). The dialer is the injection
// seam: production passes NewSPDYDialer(restConfig); tests pass a fake. The pool
// holds the dialer but starts no goroutine until the first port-forward dial.
func NewResolver(cfg *config.Config, k8s client.Client, dialer TunnelDialer) *Resolver {
	return &Resolver{
		cfg:       cfg,
		k8s:       k8s,
		pool:      newPool(dialer, defaultIdleTimeout, time.Now),
		inCluster: detectInCluster,
	}
}

// detectInCluster reports whether the process is running inside a Kubernetes pod.
// rest.InClusterConfig() checks both the KUBERNETES_SERVICE_* env vars and the
// service-account token/CA mount, returning rest.ErrNotInCluster otherwise — a
// more complete signal than reading the env var alone.
func detectInCluster() bool {
	_, err := rest.InClusterConfig()
	return err == nil
}

// mode resolves the strategy from cfg.RayAccess (already enum-validated to
// auto|direct|port-forward by config.Load). An explicit override short-circuits
// the in-cluster probe entirely.
func (r *Resolver) mode() accessMode {
	switch r.cfg.RayAccess {
	case "direct":
		return modeDirect
	case "port-forward":
		return modePortForward
	default: // "auto"
		if r.inCluster() {
			return modeDirect
		}
		return modePortForward
	}
}

// Endpoint returns a usable base URL for the named cluster's head dashboard on
// the given port (8265 for the Job Submission REST API). DirectDial returns the
// in-cluster service URL; PortForward returns a pooled local tunnel URL.
func (r *Resolver) Endpoint(ctx context.Context, namespace, cluster string, port int) (domain.Endpoint, error) {
	if port <= 0 {
		port = defaultDashboardPort
	}

	switch r.mode() {
	case modeDirect:
		return r.directEndpoint(ctx, namespace, cluster, port)
	default:
		return r.pooledEndpoint(ctx, namespace, cluster, port)
	}
}

// directEndpoint resolves the in-cluster head service URL. Per C2 (spec §7.B) it
// reads the operator-written rc.Status.Head.ServiceName rather than templating a
// DNS name — KubeRay's head-service naming is version-dependent and RayService
// cluster names are generated, so a template would be wrong. This replicates the
// formula in kuberay/cluster.go's dashboardURL (kept independent so it honors the
// caller's port rather than that file's hardcoded 8265). An unresolved or
// not-yet-provisioned head service degrades to a typed unreachable error (§10);
// a k8s Get failure (RBAC/connect) collapses to the same unreachable error by
// design rather than the kuberay adapter's NotFound/Forbidden taxonomy.
func (r *Resolver) directEndpoint(ctx context.Context, namespace, cluster string, port int) (domain.Endpoint, error) {
	var rc rayv1.RayCluster
	if err := r.k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: cluster}, &rc); err != nil {
		return domain.Endpoint{}, &domain.RayAPIUnreachableError{
			Endpoint: fmt.Sprintf("%s/%s", namespace, cluster),
			Reason:   fmt.Sprintf("resolve head service: %v", err),
		}
	}

	svc := rc.Status.Head.ServiceName
	if svc == "" {
		return domain.Endpoint{}, &domain.RayAPIUnreachableError{
			Endpoint: fmt.Sprintf("%s/%s", namespace, cluster),
			Reason:   "head service not yet provisioned",
		}
	}

	return domain.Endpoint{BaseURL: fmt.Sprintf("http://%s.%s.svc:%d", svc, namespace, port)}, nil
}

// pooledEndpoint resolves (and pools) an SPDY port-forward tunnel to the head
// pod. The idle reaper is started on first use (lazily), not at construction.
func (r *Resolver) pooledEndpoint(ctx context.Context, namespace, cluster string, port int) (domain.Endpoint, error) {
	r.ensureReaper()

	baseURL, err := r.pool.endpoint(ctx, poolKey{namespace, cluster}, port, func() (string, error) {
		return r.headPodName(ctx, namespace, cluster)
	})
	if err != nil {
		return domain.Endpoint{}, err
	}
	return domain.Endpoint{BaseURL: baseURL}, nil
}

// ensureReaper starts the pool's idle-reaper goroutine exactly once, on the first
// port-forward use. DirectDial-only resolvers never start it.
func (r *Resolver) ensureReaper() {
	r.reaperOnce.Do(func() {
		go r.pool.runReaper()
	})
}

// Close tears down all pooled tunnels and stops the reaper. It is safe to call on
// a DirectDial-only resolver (the pool is empty and the reaper never started).
func (r *Resolver) Close() {
	r.pool.close()
}
