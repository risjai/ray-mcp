// Tier 1 (unit) coverage for the KubeRay adapter's lazy-dial behavior. These
// tests run in the fast no-tags loop: they prove the server-boot invariant that
// NewClient never touches the network and never fails, and that a cluster call
// against an unresolvable kubeconfig returns a clean error rather than panicking.
// They deliberately point at a nonexistent kubeconfig so no real cluster is
// required.
package kuberay

import (
	"context"
	"strings"
	"testing"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// bogusConfig returns a config whose kubeconfig path does not exist, so any
// attempt to resolve a rest.Config from it fails locally (no network, no
// cluster). It also pins an explicit context so ContextName() is deterministic
// without reading the developer's real kubeconfig.
func bogusConfig() *config.Config {
	return &config.Config{
		Kubeconfig:       "/nonexistent/path/to/kubeconfig",
		Context:          "no-such-context",
		DefaultNamespace: "ray-system",
	}
}

// TestNewClientNeverDialsOrFails proves the boot invariant: NewClient is pure
// (no network, no error return) even with a nonexistent kubeconfig, and reports
// the config-derived binding fields that ray_capabilities surfaces — all without
// a cluster.
func TestNewClientNeverDialsOrFails(t *testing.T) {
	t.Parallel()

	c := NewClient(bogusConfig())
	if c == nil {
		t.Fatal("NewClient returned nil")
	}
	if got := c.ContextName(); got != "no-such-context" {
		t.Errorf("ContextName() = %q, want the bound --context %q", got, "no-such-context")
	}
	if got := c.DefaultNamespace(); got != "ray-system" {
		t.Errorf("DefaultNamespace() = %q, want %q", got, "ray-system")
	}
}

// TestContextNamePlaceholderWhenUnresolvable proves ContextName falls back to the
// placeholder when no --context is set and the local kubeconfig cannot be read
// (here a nonexistent path) — never a panic, never an apiserver call.
func TestContextNamePlaceholderWhenUnresolvable(t *testing.T) {
	t.Parallel()

	cfg := bogusConfig()
	cfg.Context = "" // force the RawConfig() best-effort read, which fails on the bogus path.

	c := NewClient(cfg)
	if got := c.ContextName(); got != currentContextPlaceholder {
		t.Errorf("ContextName() = %q, want placeholder %q", got, currentContextPlaceholder)
	}
}

// TestListClustersUnresolvableKubeconfigCleanError proves a cluster call against
// an unresolvable kubeconfig returns a clean, wrapped error (not a panic, not a
// nil-pointer dereference of the never-built client). The message must make clear
// it is a connect/config problem.
func TestListClustersUnresolvableKubeconfigCleanError(t *testing.T) {
	t.Parallel()

	c := NewClient(bogusConfig())

	_, err := c.ListClusters(context.Background(), "ray-system", domain.ListOptions{})
	if err == nil {
		t.Fatal("ListClusters with an unresolvable kubeconfig returned nil error, want a clean connect error")
	}
	if !strings.Contains(err.Error(), "cannot reach cluster") {
		t.Errorf("error %q does not signal a connect/config problem", err.Error())
	}
}

// TestGetClusterUnresolvableKubeconfigCleanError mirrors the list case for get.
func TestGetClusterUnresolvableKubeconfigCleanError(t *testing.T) {
	t.Parallel()

	c := NewClient(bogusConfig())

	_, err := c.GetCluster(context.Background(), "ray-system", "whatever")
	if err == nil {
		t.Fatal("GetCluster with an unresolvable kubeconfig returned nil error, want a clean connect error")
	}
	if !strings.Contains(err.Error(), "cannot reach cluster") {
		t.Errorf("error %q does not signal a connect/config problem", err.Error())
	}
}
