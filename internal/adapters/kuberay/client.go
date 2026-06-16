package kuberay

import "github.com/risjai/ray-mcp/internal/config"

// currentContextPlaceholder is reported when no --context is bound. Task 4 makes
// no Kubernetes API call and does not parse the kubeconfig, so it cannot resolve
// the actual current-context name; this placeholder is honest about that. Task 5
// grows Client into the real controller-runtime client and can resolve/validate
// the bound context then.
const currentContextPlaceholder = "(current-context)"

// Client is the KubeRay adapter. In Task 4 (the walking skeleton) it is a thin
// seam that only holds the bound kubeconfig context name and default namespace
// resolved from config — it does NOT dial the API server, build a
// controller-runtime client, or import the KubeRay typed apis. Task 5 grows this
// into the real uncached controller-runtime client implementing
// domain.KubeRayPort. Keeping it config-only here is what lets ray_capabilities
// report cluster binding without any network call.
type Client struct {
	contextName      string
	defaultNamespace string
}

// NewClient builds the skeleton adapter from config. It performs no I/O: the
// context name is taken verbatim from cfg.Context (falling back to a placeholder
// when empty, since the real current-context is not resolved until Task 5), and
// the default namespace is taken from cfg.DefaultNamespace.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		contextName:      cfg.Context,
		defaultNamespace: cfg.DefaultNamespace,
	}
}

// ContextName returns the bound kubeconfig context name, or a placeholder when
// no --context was set (the real current-context is resolved in Task 5).
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
