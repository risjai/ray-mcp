package reachability

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// Head-pod selector labels (KubeRay v1.6.1, operator-stamped and non-overridable).
// clusterLabel alone matches every pod of the cluster (head + workers); the
// port-forward target is the head pod specifically, so node-type narrows it.
const (
	clusterLabel  = "ray.io/cluster"
	nodeTypeLabel = "ray.io/node-type"
	headNodeType  = "head"
)

// TunnelDialer opens one port-forward tunnel to a pod and returns a handle whose
// Close tears it down. It is the seam that isolates the real client-go SPDY
// plumbing: production uses spdyDialer (below); unit tests pass a fake so pooling,
// reaping and re-dial are exercised with no cluster and no real SPDY stream.
type TunnelDialer interface {
	Dial(ctx context.Context, namespace, podName string, remotePort int) (TunnelHandle, error)
}

// TunnelHandle is one live port-forward: the OS-assigned local port, a channel
// that closes when the tunnel drops (lost pod connection / forwarder exit), and
// Close to tear it down. Lost drives the pool's re-dial-once behavior. The handle
// is deliberately opaque — it never exposes the underlying forwarder, so the
// read-only RayAPIPort boundary cannot be bypassed (Q6).
type TunnelHandle interface {
	LocalPort() int
	Lost() <-chan struct{}
	Close()
}

// spdyDialer is the production TunnelDialer. It builds an SPDY round tripper from
// the rest.Config and opens a port-forward to the pod's portforward subresource.
type spdyDialer struct {
	rest *rest.Config
}

// NewSPDYDialer builds the production dialer from the resolved rest.Config (the
// same config the kuberay adapter resolves from the kubeconfig).
func NewSPDYDialer(restConfig *rest.Config) TunnelDialer {
	return &spdyDialer{rest: restConfig}
}

// spdyTunnel is the live forwarder handle returned by spdyDialer.Dial.
type spdyTunnel struct {
	localPort int
	stop      chan struct{} // closed by Close to unblock ForwardPorts and tear down listeners.
	lost      chan struct{} // closed when ForwardPorts returns (tunnel dropped or stopped).
}

func (t *spdyTunnel) LocalPort() int        { return t.localPort }
func (t *spdyTunnel) Lost() <-chan struct{} { return t.lost }
func (t *spdyTunnel) Close()                { closeOnce(t.stop) }

// Dial opens an SPDY port-forward from an OS-assigned local port to remotePort on
// the named pod, returning once the listener is ready. Forwarding runs in a
// background goroutine; when it returns (dropped connection or Close), lost is
// closed so the pool can re-dial once.
func (d *spdyDialer) Dial(ctx context.Context, namespace, podName string, remotePort int) (TunnelHandle, error) {
	u, err := url.Parse(fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s/portforward", d.rest.Host, namespace, podName))
	if err != nil {
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: fmt.Sprintf("build portforward url: %v", err)}
	}

	rt, upgrader, err := spdy.RoundTripperFor(d.rest)
	if err != nil {
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: fmt.Sprintf("spdy round tripper: %v", err)}
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: rt}, "POST", u)

	stop := make(chan struct{})
	ready := make(chan struct{})
	// Forward by NUMBER (remotePort), not a named container port: in v1.6.1 the
	// dashboard port is exposed via rayStartParams, so a named "dashboard" port
	// exists only when the user declared it. "0:remote" asks for an OS-assigned
	// local port. Explicit 127.0.0.1 avoids "localhost" binding both v4 and v6.
	pf, err := portforward.NewOnAddresses(
		dialer,
		[]string{"127.0.0.1"},
		[]string{fmt.Sprintf("0:%d", remotePort)},
		stop, ready,
		io.Discard, io.Discard,
	)
	if err != nil {
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: fmt.Sprintf("build port forwarder: %v", err)}
	}

	lost := make(chan struct{})
	go func() {
		// ForwardPorts blocks until stop is closed or the connection drops; either
		// way the tunnel is no longer usable, so signal lost.
		_ = pf.ForwardPorts()
		close(lost)
	}()

	select {
	case <-ready:
	case <-ctx.Done():
		closeOnce(stop)
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: fmt.Sprintf("port forward not ready: %v", ctx.Err())}
	case <-lost:
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: "port forward failed before ready"}
	}

	ports, err := pf.GetPorts()
	if err != nil || len(ports) == 0 {
		closeOnce(stop)
		return nil, &domain.RayAPIUnreachableError{Endpoint: podName, Reason: fmt.Sprintf("resolve local port: %v", err)}
	}

	return &spdyTunnel{localPort: int(ports[0].Local), stop: stop, lost: lost}, nil
}

// headPodName resolves the head pod for (namespace, cluster) by the
// operator-stamped labels and returns the first Running one. Selecting on
// clusterLabel alone would also match worker pods, so node-type=head narrows it
// to the dashboard-hosting pod (C2: an exact, label-driven target, not a guess).
//
// k8s read failures collapse to a typed RayAPIUnreachableError by design: this
// adapter's contract is "endpoint or a reason it is unreachable" (§10 graceful
// degradation), so an RBAC/connect failure here surfaces as unreachable rather
// than the kuberay adapter's finer NotFound/Forbidden taxonomy.
func (r *Resolver) headPodName(ctx context.Context, namespace, cluster string) (string, error) {
	var pods corev1.PodList
	if err := r.k8s.List(ctx, &pods,
		client.InNamespace(namespace),
		client.MatchingLabels{clusterLabel: cluster, nodeTypeLabel: headNodeType},
	); err != nil {
		return "", &domain.RayAPIUnreachableError{
			Endpoint: fmt.Sprintf("%s/%s", namespace, cluster),
			Reason:   fmt.Sprintf("list head pod: %v", err),
		}
	}

	// Skip a Running-but-terminating pod: during a head reschedule the old pod
	// still reports Running until it is gone, and forwarding to it would just
	// drop. Preferring a non-terminating Running pod re-homes the tunnel onto the
	// replacement on the same call.
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			return pod.Name, nil
		}
	}

	return "", &domain.RayAPIUnreachableError{
		Endpoint: fmt.Sprintf("%s/%s", namespace, cluster),
		Reason:   "no running head pod",
	}
}

// closeOnce closes ch unless it is already closed, making Close idempotent under
// SEQUENTIAL calls. It is not safe for two goroutines closing the same channel
// concurrently (both could pass the default case); the safety invariant is that
// every pooled handle's Close runs while pool.mu is held (pool.endpoint/reap/
// close), and the in-Dial stop closes happen before the handle is published — so
// no concurrent close of one channel ever occurs.
func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
