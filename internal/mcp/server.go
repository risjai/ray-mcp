package mcp

import (
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/version"
)

// NewServer constructs the MCP server and registers the tools available for the
// given config. For Task 4 (the walking skeleton) that is only the read-only
// ray_capabilities meta tool, whose cluster-binding fields come from src (the
// kuberay skeleton adapter). It returns the underlying *mcp.Server so the caller
// can run it over any transport (stdio in main, in-memory in tests).
func NewServer(cfg *config.Config, src capabilitiesSource) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "ray-mcp",
		Version: version.Version,
	}, nil)

	addCapabilitiesTool(server, cfg, src)

	return server
}
