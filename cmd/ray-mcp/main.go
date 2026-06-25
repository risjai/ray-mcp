// Command ray-mcp is an MCP server for managing Ray on Kubernetes via KubeRay.
//
// It exposes tools to manage the lifecycle of RayCluster, RayJob, and
// RayService resources through KubeRay CRDs (the guarded write path) and
// reaches Ray's dashboard/job API read-only for the runtime detail the CRDs do
// not expose (the cross-plane "wedge").
//
// Task 4 wires the walking skeleton: config -> minimal kuberay adapter -> MCP
// server (one read-only meta tool) -> stdio transport. Under stdio, stdout is
// the JSON-RPC wire, so all diagnostics go to stderr.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/risjai/ray-mcp/internal/adapters/kuberay"
	"github.com/risjai/ray-mcp/internal/adapters/rayapi"
	"github.com/risjai/ray-mcp/internal/adapters/reachability"
	"github.com/risjai/ray-mcp/internal/config"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
	"github.com/risjai/ray-mcp/internal/observability"
	"github.com/risjai/ray-mcp/internal/transport"
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load(os.Args[1:])
	if err != nil {
		// stdio invariant: diagnostics go to stderr, never stdout.
		fmt.Fprintf(os.Stderr, "ray-mcp: %v\n", err)
		return 1
	}

	// Logger is bound to stderr so it never corrupts the stdout JSON-RPC wire.
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel(cfg.LogLevel)}))

	if cfg.Transport != "stdio" {
		logger.Error("transport not yet implemented", "transport", cfg.Transport)
		return 1
	}

	// Build the adapter WITHOUT dialing: the controller-runtime client is built
	// lazily on the first ray_cluster_* call. This lets the server always boot —
	// and the cluster-free ray_capabilities tool work — even when no kubeconfig
	// can be resolved; cluster tools return a clean error if the cluster is
	// unreachable.
	adapter := kuberay.NewClient(cfg)
	// Audit sink for the mutation choke point (Task 8b): newline-delimited JSON to
	// stderr, never stdout (the stdio JSON-RPC wire). Wired here even when mutations
	// are disabled — NewServer only registers the audited write tools under
	// --allow-mutations, so an unmutated server simply never emits a record.
	audit := observability.NewAuditLogger(os.Stderr)

	// Wire the cross-plane "wedge" backend (RayJob read tools). The reachability
	// resolver and SPDY dialer need a live cluster connection, so they reuse the
	// kuberay adapter's kubeconfig resolution. This is best-effort and NON-fatal:
	// if no kubeconfig resolves (e.g. no cluster bound), the wedge tools are still
	// advertised but return a clean unreachable error at call time — the server
	// boots and ray_capabilities works regardless (the lazy-dial boot invariant).
	wedge := buildWedge(cfg, adapter, logger)

	server := mcpserver.NewServer(cfg, adapter, adapter, adapter, wedge, audit)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting ray-mcp", "transport", cfg.Transport, "defaultNamespace", cfg.DefaultNamespace)
	if err := transport.RunStdio(ctx, server); err != nil {
		logger.Error("server exited with error", "err", err)
		return 1
	}
	return 0
}

// buildWedge assembles the cross-plane "wedge" backend for the RayJob read tools
// (ray_job_get / ray_job_logs): the phase-1 CRD reader (the kuberay adapter), the
// head-endpoint resolver, and the read-only Ray dashboard client.
//
// The resolver and SPDY dialer need a live cluster connection, so they reuse the
// kuberay adapter's kubeconfig resolution via RuntimeClient. That resolution is
// best-effort here and NON-fatal: when it fails (no kubeconfig / no cluster
// bound), the wedge still registers but with a degraded resolver that reports the
// dashboard unreachable — preserving the lazy-dial boot invariant (the server
// boots and ray_capabilities answers with no cluster). Jobs is always the
// adapter, so the phase-1 CRD read surfaces its own clean error if the cluster is
// unreachable; only phase 2 (the dashboard dial) is degraded.
func buildWedge(cfg *config.Config, adapter *kuberay.Client, logger *slog.Logger) mcpserver.WedgeBackend {
	api := rayapi.NewClient(cfg)

	k8s, restConfig, err := adapter.RuntimeClient()
	if err != nil {
		logger.Warn("wedge reachability degraded: no live cluster connection; ray_job_* live status will report the dashboard unreachable until a cluster is bound", "err", err)
		return mcpserver.WedgeBackend{
			Jobs:  adapter,
			Reach: reachability.NewUnavailable(fmt.Sprintf("no cluster connection: %v", err)),
			API:   api,
		}
	}

	resolver := reachability.NewResolver(cfg, k8s, reachability.NewSPDYDialer(restConfig))
	return mcpserver.WedgeBackend{Jobs: adapter, Reach: resolver, API: api}
}

// logLevel maps the validated config log level to an slog level.
func logLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
