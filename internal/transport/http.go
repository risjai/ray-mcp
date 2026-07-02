package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
)

// RunHTTP serves the MCP server over the streamable-HTTP transport, blocking
// until ctx is cancelled (then draining in-flight requests) or the listener
// fails. It binds cfg.HTTPAddr and wraps the go-sdk handler with the configured
// auth. It returns nil on a clean, context-triggered shutdown.
func RunHTTP(ctx context.Context, cfg *config.Config, srv *mcp.Server) error {
	handler, err := buildHTTPHandler(cfg, srv)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		return fmt.Errorf("bind %q: %w", cfg.HTTPAddr, err)
	}
	return serve(ctx, ln, handler)
}

// buildHTTPHandler assembles the HTTP handler: the go-sdk streamable handler
// (serving the one server for every session) wrapped in the configured auth.
//
// It re-asserts the bind/auth boot invariant (Q8) at the transport edge — the
// same rule config.validate enforces, repeated here as defense in depth so the
// code that actually binds a socket is itself the thing that refuses to serve
// insecurely. It also refuses auth modes the transport cannot yet enforce
// (tokenreview arrives in Task 24): advertising a mode without enforcing it
// would be an unauthenticated bind.
func buildHTTPHandler(cfg *config.Config, srv *mcp.Server) (http.Handler, error) {
	base := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)

	switch cfg.AuthMode {
	case "static":
		if cfg.AuthToken != "" {
			return staticBearerAuth(cfg.AuthToken, base), nil
		}
		// No token: only a loopback bind may serve unauthenticated (OS-gated).
		if !isLoopback(cfg.HTTPAddr) {
			return nil, fmt.Errorf(
				"refusing to serve: --http-addr %q binds a non-loopback address without a static --auth-token",
				cfg.HTTPAddr,
			)
		}
		return base, nil
	case "tokenreview":
		return nil, errors.New("--auth-mode tokenreview is not yet implemented (arrives in Task 24); use static bearer auth")
	default:
		return nil, fmt.Errorf("unsupported --auth-mode %q", cfg.AuthMode)
	}
}

// serve runs an http.Server on ln until ctx is cancelled, then gracefully drains
// in-flight requests. It returns nil on a clean shutdown; http.ErrServerClosed
// is treated as clean.
func serve(ctx context.Context, ln net.Listener, handler http.Handler) error {
	srv := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("http serve: %w", err)
	}
}

// isLoopback reports whether host:port binds a loopback address. It mirrors
// config.isLoopback (kept private to each package to avoid a transport→config
// coupling on an internal helper); the two must agree on what "loopback" means.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	switch host {
	case "":
		return false
	case "localhost":
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}
