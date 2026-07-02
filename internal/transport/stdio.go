// Package transport runs an MCP server over a chosen transport: stdio (primary)
// or streamable HTTP (with auth). Under stdio, stdout IS the JSON-RPC wire — the
// transport writes protocol frames there — so all logging and diagnostics must
// go to stderr or a file, never stdout.
package transport

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RunStdio runs the server over the stdio transport, blocking until the client
// closes the connection or ctx is cancelled. The go-sdk's StdioTransport reads
// JSON-RPC from stdin and writes responses to stdout; the caller must keep
// stdout free of any other output.
func RunStdio(ctx context.Context, srv *mcp.Server) error {
	if err := srv.Run(ctx, &mcp.StdioTransport{}); err != nil {
		return fmt.Errorf("run stdio transport: %w", err)
	}
	return nil
}
