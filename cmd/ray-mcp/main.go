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
	server := mcpserver.NewServer(cfg, adapter, adapter, adapter, audit)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("starting ray-mcp", "transport", cfg.Transport, "defaultNamespace", cfg.DefaultNamespace)
	if err := transport.RunStdio(ctx, server); err != nil {
		logger.Error("server exited with error", "err", err)
		return 1
	}
	return 0
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
