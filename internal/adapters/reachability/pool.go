package reachability

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// pool caches SPDY port-forward tunnels per (namespace, cluster) so repeated
// dashboard reads reuse one tunnel instead of re-establishing it each call (spec
// §7.A, Q6). A warm tunnel is reused; a dropped one is re-dialed exactly once; an
// idle one is closed by the reaper.
type pool struct {
	mu      sync.Mutex
	dialer  TunnelDialer
	idle    time.Duration
	now     func() time.Time // clock seam: defaults to time.Now, overridden in tests.
	tunnels map[poolKey]*entry

	done chan struct{} // closed by close() to stop the reaper goroutine.
}

// poolKey identifies a tunnel by the cluster it reaches (spec: pooled per
// (namespace, cluster)).
type poolKey struct {
	namespace string
	cluster   string
}

// entry is one pooled tunnel plus its derived base URL and last-use time.
type entry struct {
	handle   TunnelHandle
	baseURL  string
	lastUsed time.Time
}

func newPool(dialer TunnelDialer, idle time.Duration, now func() time.Time) *pool {
	return &pool{
		dialer:  dialer,
		idle:    idle,
		now:     now,
		tunnels: make(map[poolKey]*entry),
		done:    make(chan struct{}),
	}
}

// endpoint returns the local base URL for key's tunnel, reusing a warm one or
// dialing a fresh one. resolvePod resolves the head pod name to forward to (only
// called when a dial is needed). A dropped tunnel is torn down and re-dialed
// exactly once per call; a failed dial returns a typed unreachable error so the
// domain degrades gracefully (§10) rather than this looping.
//
// The mutex is held across the dial. Dials are rare (once per cluster per idle
// window) and the cost keeps the map and the dial atomic, mirroring the kuberay
// adapter's hold-lock-across-lazy-build precedent.
func (p *pool) endpoint(ctx context.Context, key poolKey, remotePort int, resolvePod func() (string, error)) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if e, ok := p.tunnels[key]; ok {
		if alive(e.handle) {
			e.lastUsed = p.now()
			return e.baseURL, nil
		}
		// Dropped tunnel: tear it down and fall through to a single re-dial.
		e.handle.Close()
		delete(p.tunnels, key)
	}

	pod, err := resolvePod()
	if err != nil {
		return "", err
	}

	handle, err := p.dialer.Dial(ctx, key.namespace, pod, remotePort)
	if err != nil {
		return "", err
	}

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", handle.LocalPort())
	p.tunnels[key] = &entry{handle: handle, baseURL: baseURL, lastUsed: p.now()}
	return baseURL, nil
}

// alive reports whether a tunnel handle is still usable (its Lost channel has not
// closed). The check is non-blocking.
func alive(h TunnelHandle) bool {
	select {
	case <-h.Lost():
		return false
	default:
		return true
	}
}

// reap closes every tunnel idle for at least p.idle as of now and returns how
// many were closed. It is the pure step the production reaper goroutine calls on
// a ticker and that tests call directly with a fake clock — so idle-reaping is
// proven without sleeps or a real timer.
func (p *pool) reap(now time.Time) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	closed := 0
	for key, e := range p.tunnels {
		if now.Sub(e.lastUsed) >= p.idle {
			e.handle.Close()
			delete(p.tunnels, key)
			closed++
		}
	}
	return closed
}

// runReaper drives reap on a ticker until close() stops it. It is the only
// untested production glue (a thin clock source over the tested reap step); the
// tick interval is half the idle window so a tunnel is reaped within ~1.5× idle.
func (p *pool) runReaper() {
	t := time.NewTicker(p.idle / 2)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			p.reap(p.now())
		case <-p.done:
			return
		}
	}
}

// close stops the reaper and tears down every pooled tunnel. It is idempotent.
func (p *pool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	closeOnce(p.done)
	for key, e := range p.tunnels {
		e.handle.Close()
		delete(p.tunnels, key)
	}
}
