package mcp

import (
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/version"
)

// ClusterWriteBackend is the write slice NewServer needs to register the mutating
// RayCluster tools: the curated→base builder (spec §7.C step 1) and the SSA Applier
// (Task 8b). The KubeRay adapter satisfies both; they are bundled into one narrow
// interface so NewServer takes a single backend handle (the adapter) rather than a
// growing parameter list, and tests can inject a fake. It is only consulted when
// cfg.AllowMutations is set.
type ClusterWriteBackend interface {
	domain.ClusterBaseBuilder
	domain.Applier
}

// NewServer constructs the MCP server and registers the tools available for the
// given config. The read-only ray_capabilities meta tool reports cluster binding
// from src (config-only, no cluster call); the RayCluster read tools
// (ray_cluster_list / ray_cluster_get / ray_cluster_events) read a live cluster via
// kube. The mutating tools (ray_cluster_create, ...) register ONLY when
// cfg.AllowMutations is set (spec §6: disabled tools are not advertised); they run
// through the unified apply pipeline (write backend) and emit one audit record per
// mutation via audit. All collaborators are narrow interfaces so tests can inject
// fakes. It returns the underlying *mcp.Server so the caller can run it over any
// transport (stdio in main, in-memory in tests).
func NewServer(cfg *config.Config, src capabilitiesSource, kube domain.ClusterReader, write ClusterWriteBackend, audit domain.AuditSink) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "ray-mcp",
		Version: version.Version,
	}, nil)

	addCapabilitiesTool(server, cfg, src)
	addClusterTools(server, domain.NewClusterService(kube, cfg.DefaultNamespace))

	if cfg.AllowMutations {
		applySvc := domain.NewApplyService(write, audit)
		writeSvc := domain.NewClusterWriteService(write, kube, applySvc, cfg.DefaultNamespace)
		addClusterWriteTools(server, writeSvc, cfg.AllowRawSpec, cfg.AllowDestructive)
	}

	return server
}
