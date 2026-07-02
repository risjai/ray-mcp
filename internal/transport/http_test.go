package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
)

// testMCPServer builds a minimal but real go-sdk MCP server with one tool. The
// transport's contract is to carry whatever *mcp.Server it is handed faithfully;
// main.go passes the SAME server object to RunStdio or RunHTTP, so proving the
// transport carries a real session end-to-end is what "same tools as stdio"
// reduces to at this layer.
func testMCPServer() *mcp.Server {
	s := mcp.NewServer(&mcp.Implementation{Name: "ray-mcp-test", Version: "v0"}, nil)
	mcp.AddTool(s, &mcp.Tool{
		Name:        "ping",
		Description: "test tool",
	}, func(_ context.Context, _ *mcp.CallToolRequest, _ struct{}) (*mcp.CallToolResult, struct{}, error) {
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: "pong"}}}, struct{}{}, nil
	})
	return s
}

// bearerRoundTripper injects a static bearer token onto every outgoing request.
type bearerRoundTripper struct {
	token string
	base  http.RoundTripper
}

func (b bearerRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if b.token != "" {
		r.Header.Set("Authorization", "Bearer "+b.token)
	}
	return b.base.RoundTrip(r)
}

func httpClientWithToken(token string) *http.Client {
	return &http.Client{Transport: bearerRoundTripper{token: token, base: http.DefaultTransport}}
}

func TestBuildHTTPHandlerRefusesTokenReview(t *testing.T) {
	// tokenreview auth is not built until Task 24. config.validate lets it pass
	// (it counts as "has auth"), so the transport MUST refuse rather than serve a
	// mode it cannot actually enforce — that would be an unauthenticated bind.
	cfg := &config.Config{Transport: "http", HTTPAddr: "0.0.0.0:8765", AuthMode: "tokenreview"}
	_, err := buildHTTPHandler(cfg, testMCPServer())
	if err == nil {
		t.Fatal("buildHTTPHandler accepted tokenreview mode; want refusal until Task 24")
	}
}

func TestBuildHTTPHandlerRefusesNonLoopbackWithoutToken(t *testing.T) {
	// Defense in depth over config.validate: a non-loopback bind with no
	// enforceable auth must never yield a servable handler.
	cfg := &config.Config{Transport: "http", HTTPAddr: "0.0.0.0:8765", AuthMode: "static", AuthToken: ""}
	_, err := buildHTTPHandler(cfg, testMCPServer())
	if err == nil {
		t.Fatal("buildHTTPHandler accepted a non-loopback bind without a token")
	}
}

func TestBuildHTTPHandlerAllowsLoopbackWithoutToken(t *testing.T) {
	// Loopback is OS-gated, so an unauthenticated loopback bind is allowed (local
	// dev parity with stdio).
	cfg := &config.Config{Transport: "http", HTTPAddr: "127.0.0.1:8765", AuthMode: "static", AuthToken: ""}
	h, err := buildHTTPHandler(cfg, testMCPServer())
	if err != nil {
		t.Fatalf("buildHTTPHandler rejected a loopback tokenless bind: %v", err)
	}
	if h == nil {
		t.Fatal("buildHTTPHandler returned a nil handler")
	}
}

// serveTestSession starts serve() on an ephemeral loopback port and connects an
// MCP client over streamable HTTP using the given client-side token. It returns
// the live client session; the server is shut down via t.Cleanup.
func serveTestSession(t *testing.T, cfg *config.Config, clientToken string) (*mcp.ClientSession, error) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg.HTTPAddr = ln.Addr().String()

	handler, err := buildHTTPHandler(cfg, testMCPServer())
	if err != nil {
		t.Fatalf("buildHTTPHandler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- serve(ctx, ln, handler) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-served:
		case <-time.After(5 * time.Second):
			t.Error("serve did not return after context cancel")
		}
	})

	transport := &mcp.StreamableClientTransport{
		Endpoint:   "http://" + cfg.HTTPAddr,
		HTTPClient: httpClientWithToken(clientToken),
		MaxRetries: -1, // fail fast on 401 rather than retrying
	}
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "v0"}, nil)
	connectCtx, connectCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer connectCancel()
	session, err := client.Connect(connectCtx, transport, nil)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() { _ = session.Close() })
	return session, nil
}

func TestHTTPServesToolsWithValidToken(t *testing.T) {
	cfg := &config.Config{Transport: "http", AuthMode: "static", AuthToken: "s3cret"}
	session, err := serveTestSession(t, cfg, "s3cret")
	if err != nil {
		t.Fatalf("connect with valid token: %v", err)
	}

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	if !slices.Contains(names, "ping") {
		t.Fatalf("tool not carried over HTTP; got %v", names)
	}
}

func TestHTTPRejectsMissingToken(t *testing.T) {
	cfg := &config.Config{Transport: "http", AuthMode: "static", AuthToken: "s3cret"}
	// Client sends no token: the initialize handshake must fail with 401.
	if _, err := serveTestSession(t, cfg, ""); err == nil {
		t.Fatal("connected without a token; want 401 rejection")
	}
}

func TestRunHTTPReturnsOnContextCancel(t *testing.T) {
	cfg := &config.Config{Transport: "http", HTTPAddr: "127.0.0.1:0", AuthMode: "static", AuthToken: ""}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- RunHTTP(ctx, cfg, testMCPServer()) }()

	// Give the listener a moment to bind, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("RunHTTP returned %v, want clean shutdown", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("RunHTTP did not return after context cancel")
	}
}

func TestRunHTTPPropagatesBuildError(t *testing.T) {
	// The tokenreview refusal must propagate through the real entrypoint, not
	// just buildHTTPHandler in isolation.
	cfg := &config.Config{Transport: "http", HTTPAddr: "127.0.0.1:0", AuthMode: "tokenreview"}
	if err := RunHTTP(context.Background(), cfg, testMCPServer()); err == nil {
		t.Fatal("RunHTTP served tokenreview mode; want refusal")
	}
}
