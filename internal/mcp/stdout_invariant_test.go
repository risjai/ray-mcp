package mcp_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// TestStdoutStaysClean asserts the stdio invariant: under stdio transport,
// stdout IS the JSON-RPC wire, so no diagnostics may land there. The test runs
// the full ray_capabilities round-trip over in-memory transports (which carry
// the protocol off-stdout) while os.Stdout is redirected to a pipe, and asserts
// nothing was written to stdout. As a positive control it also emits a log line
// through a stderr-bound slog logger (the same wiring main.go uses) during the
// captured window and confirms it does NOT appear on the captured stdout.
//
// This test must not run in parallel: it swaps the process-global os.Stdout.
func TestStdoutStaysClean(t *testing.T) {
	orig := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = w
	t.Cleanup(func() { os.Stdout = orig })

	captured := make(chan []byte, 1)
	go func() {
		b, _ := io.ReadAll(r)
		captured <- b
	}()

	ctx := context.Background()
	cfg := &config.Config{DefaultNamespace: "default", AllowMutations: true}

	// Logger wired exactly as main.go wires it: to stderr, never stdout.
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	server := mcpserver.NewServer(cfg, stdoutFakeSource{})
	serverT, clientT := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverT, nil); err != nil {
		t.Fatalf("server.Connect: %v", err)
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	session, err := client.Connect(ctx, clientT, nil)
	if err != nil {
		t.Fatalf("client.Connect: %v", err)
	}

	logger.Info("diagnostic during capabilities call: this must go to stderr")

	if _, err := session.CallTool(ctx, &mcp.CallToolParams{
		Name:      "ray_capabilities",
		Arguments: map[string]any{},
	}); err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	_ = session.Close()

	// Close the write end so the reader goroutine finishes.
	os.Stdout = orig
	_ = w.Close()
	got := <-captured

	if len(got) != 0 {
		t.Fatalf("stdio invariant violated: %d bytes written to stdout: %q", len(got), got)
	}
}

type stdoutFakeSource struct{}

func (stdoutFakeSource) ContextName() string      { return "ctx" }
func (stdoutFakeSource) DefaultNamespace() string { return "default" }
