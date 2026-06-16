package kuberay

import (
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/config"
)

// currentContextPlaceholder is reported when no --context is bound and no real
// client has been dialed to resolve the actual current-context name. It is
// honest about that uncertainty.
const currentContextPlaceholder = "(current-context)"

// Client is the KubeRay adapter implementing domain.KubeRayPort. It holds the
// bound kubeconfig context name and default namespace (for the config-only
// ray_capabilities path) plus an uncached controller-runtime client (k8s) for
// the real read/write CRD operations. We are a client, not a controller, so the
// client is uncached: no manager, no informer cache.
//
// k8s is nil on the config-only path (NewClient), which keeps ray_capabilities
// able to report cluster binding without any network call; it is populated by
// newRuntimeClient once a *rest.Config has been resolved and a real client
// built.
type Client struct {
	contextName      string
	defaultNamespace string
	k8s              client.Client
}

// NewClient builds the config-only adapter. It performs no I/O: the context name
// is taken verbatim from cfg.Context (falling back to a placeholder when empty,
// since the real current-context is not resolved without dialing), and the
// default namespace is taken from cfg.DefaultNamespace. The controller-runtime
// client is left nil — this is the path ray_capabilities uses, and it must not
// require a reachable apiserver.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		contextName:      cfg.Context,
		defaultNamespace: cfg.DefaultNamespace,
		k8s:              nil,
	}
}

// newRuntimeClient builds an adapter backed by an already-constructed uncached
// controller-runtime client. It is the seam tests (and later the production dial
// path) use to inject a real client.Client without NewClient having to perform
// I/O. The client's scheme must have the KubeRay v1 types registered (see
// newScheme).
//
// Its only current caller is the envtest tier (build tag envtest), so the
// default build's unused-linter cannot see it; the production dial path that
// also calls it lands in a later task.
//
//nolint:unused // exercised by the envtest-tagged tests; production dial wiring lands later.
func newRuntimeClient(contextName, defaultNamespace string, k8s client.Client) *Client {
	return &Client{
		contextName:      contextName,
		defaultNamespace: defaultNamespace,
		k8s:              k8s,
	}
}

// newScheme builds a runtime.Scheme with the KubeRay v1 types (RayCluster,
// RayJob, RayService and their lists) registered. The uncached client decodes
// typed CRD objects against this scheme.
//
//nolint:unused // exercised by the envtest-tagged tests; production dial wiring lands later.
func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := rayv1.AddToScheme(scheme); err != nil {
		return nil, err //nolint:wrapcheck // trivial scheme-registration failure; the caller has full context.
	}
	return scheme, nil
}

// ContextName returns the bound kubeconfig context name, or a placeholder when
// no --context was set.
func (c *Client) ContextName() string {
	if c.contextName == "" {
		return currentContextPlaceholder
	}
	return c.contextName
}

// DefaultNamespace returns the namespace used when a tool omits one.
func (c *Client) DefaultNamespace() string {
	return c.defaultNamespace
}
