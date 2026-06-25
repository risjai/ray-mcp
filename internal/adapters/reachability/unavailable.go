package reachability

import (
	"context"
	"fmt"

	"github.com/risjai/ray-mcp/internal/domain"
)

// Unavailable is a degraded RayReachability used when no live cluster connection
// could be resolved at startup (e.g. no kubeconfig). It satisfies the port so the
// wedge tools still register and answer, but every resolution returns a typed
// RayAPIUnreachableError carrying the boot-time reason — so a ray_job_* call on a
// scheduled job degrades cleanly (spec §10) instead of the server refusing to
// boot. The Reason is the kubeconfig-resolution error from main.go.
type Unavailable struct {
	Reason string
}

// compile-time check that the degraded resolver satisfies the port.
var _ domain.RayReachability = (*Unavailable)(nil)

// NewUnavailable builds the degraded resolver with a bounded boot-time reason.
func NewUnavailable(reason string) *Unavailable {
	return &Unavailable{Reason: reason}
}

// Endpoint always reports the cluster's head dashboard as unreachable, naming the
// boot-time reason (the same typed error a runtime dial failure would produce).
func (u *Unavailable) Endpoint(_ context.Context, namespace, cluster string, _ int) (domain.Endpoint, error) {
	return domain.Endpoint{}, &domain.RayAPIUnreachableError{
		Endpoint: fmt.Sprintf("%s/%s", namespace, cluster),
		Reason:   u.Reason,
	}
}
