package mcp

import (
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/version"
)

// NewServer constructs the MCP server and registers the tools available for the
// given config. The read-only ray_capabilities meta tool reports cluster binding
// from src (config-only, no cluster call); the RayCluster read tools
// (ray_cluster_list / ray_cluster_get) read a live cluster via kube (the dialed
// adapter's cluster read slice). Both params are narrow interfaces so tests can
// inject fakes. It returns the underlying *mcp.Server so the caller can run it
// over any transport (stdio in main, in-memory in tests).
func NewServer(cfg *config.Config, src capabilitiesSource, kube domain.ClusterReader) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "ray-mcp",
		Version: version.Version,
	}, nil)

	addCapabilitiesTool(server, cfg, src)
	addClusterTools(server, domain.NewClusterService(kube, cfg.DefaultNamespace))

	return server
}
