package kuberay

import (
	"fmt"
	"sync"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/config"
)

// currentContextPlaceholder is reported when no --context is bound and the local
// kubeconfig could not be read to resolve the actual current-context name. It is
// honest about that uncertainty.
const currentContextPlaceholder = "(current-context)"

// Client is the KubeRay adapter implementing domain.KubeRayPort. It holds the
// bound kubeconfig context name and default namespace plus an uncached
// controller-runtime client (k8s) for the real read/write CRD operations. We are
// a client, not a controller, so the client is uncached: no manager, no informer
// cache.
//
// The controller-runtime client is built LAZILY on the first cluster call (see
// ensureClient), not at construction, so the server boots — and the cluster-free
// ray_capabilities tool works — even when no kubeconfig can be resolved. cfg is
// retained for that deferred dial; mu guards the lazy build and the k8s cache.
//
// k8s is populated either lazily by ensureClient (production) or eagerly by
// newRuntimeClient (envtest) once a *rest.Config has been resolved and a real
// client built.
type Client struct {
	cfg              *config.Config
	contextName      string
	defaultNamespace string

	mu  sync.Mutex
	k8s client.Client
}

// NewClient builds the production adapter WITHOUT touching the network. It stores
// the config for a deferred dial, records the default namespace, and resolves the
// context name to report from the LOCAL merged kubeconfig (a file read, never an
// apiserver call): when --context was set that is the bound context, otherwise it
// reads the kubeconfig's current-context so ContextName() reflects the real
// binding. Any failure reading the local kubeconfig falls back to empty, which
// ContextName() renders as a placeholder. The controller-runtime client is left
// unset; ensureClient builds it on first use.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		cfg:              cfg,
		contextName:      localContextName(cfg),
		defaultNamespace: cfg.DefaultNamespace,
	}
}

// localContextName resolves the context name to report from the local kubeconfig
// without contacting the apiserver. When --context was set, that is the bound
// context. Otherwise it reads the merged raw kubeconfig's current-context
// (RawConfig is the local merge — it does NOT call ClientConfig/.ClientConfig(),
// which is the network-capable resolution). On any read error it returns the
// (empty) flag value, which ContextName() renders as a placeholder.
func localContextName(cfg *config.Config) string {
	if cfg.Context != "" {
		return cfg.Context
	}
	raw, err := loadingRulesFor(cfg).RawConfig()
	if err != nil {
		return ""
	}
	return raw.CurrentContext
}

// loadingRulesFor builds the clientcmd ClientConfig honoring cfg.Kubeconfig (the
// explicit path) and cfg.Context (the current-context override). It is shared by
// localContextName (which only reads RawConfig) and ensureClient (which resolves
// the live rest.Config).
func loadingRulesFor(cfg *config.Config) clientcmd.ClientConfig {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if cfg.Kubeconfig != "" {
		loadingRules.ExplicitPath = cfg.Kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	if cfg.Context != "" {
		overrides.CurrentContext = cfg.Context
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
}

// ensureClient returns the uncached controller-runtime client, building it lazily
// on first use. If a client is already set (a cached prior success, or one
// injected via newRuntimeClient for envtest) it is returned without dialing.
// Otherwise it resolves the *rest.Config from the kubeconfig — the step that can
// fail when no kubeconfig is present — registers the KubeRay v1 scheme, and
// builds the client (client.New is offline-safe: with the default lazy RESTMapper
// it does NOT contact the apiserver, so the network only happens on the first
// List/Get). A successful build is cached; a failure is NOT cached, so a later
// call can retry once the kubeconfig is fixed.
//
// The mutex serializes concurrent first-callers so the client is built once. The
// build is cheap/local (no network), so holding the lock across it is fine.
func (c *Client) ensureClient() (client.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.k8s != nil {
		return c.k8s, nil
	}

	restConfig, err := loadingRulesFor(c.cfg).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot reach cluster: resolve kubeconfig (context %q): %w", c.cfg.Context, err)
	}

	scheme, err := newScheme()
	if err != nil {
		return nil, fmt.Errorf("build KubeRay scheme: %w", err)
	}

	k8s, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("cannot reach cluster: build controller-runtime client: %w", err)
	}

	c.k8s = k8s
	return k8s, nil
}

// newRuntimeClient builds an adapter backed by an already-constructed uncached
// controller-runtime client. It is the seam the envtest tier uses to inject a
// real client.Client; an injected client makes ensureClient return it without
// dialing. The client's scheme must have the KubeRay v1 types registered (see
// newScheme).
//
//nolint:unused // sole caller is the envtest-tagged test (envtest injection seam); unused flags it only in the no-tags build.
func newRuntimeClient(contextName, defaultNamespace string, k8s client.Client) *Client {
	return &Client{
		contextName:      contextName,
		defaultNamespace: defaultNamespace,
		k8s:              k8s,
	}
}

// newScheme builds a runtime.Scheme with the KubeRay v1 types (RayCluster,
// RayJob, RayService and their lists) plus core/v1 registered. The uncached
// client decodes typed CRD objects against the KubeRay types; core/v1 is needed
// so ray_cluster_events can decode Pods (label-selected by ray.io/cluster) and
// Events (corev1.EventList) — controller-runtime's client does NOT register core
// types by default.
func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := rayv1.AddToScheme(scheme); err != nil {
		return nil, err //nolint:wrapcheck // trivial scheme-registration failure; the caller has full context.
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		return nil, err //nolint:wrapcheck // trivial scheme-registration failure; the caller has full context.
	}
	return scheme, nil
}

// ContextName returns the bound kubeconfig context name, or a placeholder when
// no --context was set and the local kubeconfig's current-context could not be
// read.
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
