package mcp

import (
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/version"
)

// WriteBackend is the write slice NewServer needs to register the mutating tools:
// the curated→base builders for RayCluster (spec §7.C step 1), RayJob (Task 18),
// and RayService (Task 21), the shared SSA Applier (Task 8b), and the Deleter
// (Task 12, destructive tier). The KubeRay adapter satisfies all of them; they are
// bundled into one narrow interface so NewServer takes a single backend handle (the
// adapter) rather than a growing parameter list, and tests can inject a fake. It is
// only consulted when cfg.AllowMutations is set.
type WriteBackend interface {
	domain.ClusterBaseBuilder
	domain.JobBaseBuilder
	domain.ServiceBaseBuilder
	domain.JobGetter
	domain.Applier
	domain.Deleter
}

// WedgeBackend bundles the three collaborators the RayJob read tools (the
// cross-plane "wedge", spec §5/§7.B) need: the phase-1 CRD reader (Jobs), the
// head-endpoint resolver (Reach), and the read-only Ray dashboard client (API).
// Unlike WriteBackend these are three DIFFERENT adapters, so the bundle is
// a struct, not one interface. The wedge tools register only when Jobs is
// non-nil, so a cluster-only server (or a test that does not exercise the wedge)
// simply leaves it zero — matching the "tools are advertised only when wired"
// convention (spec §6).
type WedgeBackend struct {
	Jobs  domain.JobReader
	Reach domain.RayReachability
	API   domain.RayAPIPort
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
func NewServer(cfg *config.Config, src capabilitiesSource, kube domain.ClusterReader, write WriteBackend, wedge WedgeBackend, audit domain.AuditSink) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{
		Name:    "ray-mcp",
		Version: version.Version,
	}, nil)

	addCapabilitiesTool(server, cfg, src)
	addClusterTools(server, domain.NewClusterService(kube, cfg.DefaultNamespace))

	// The RayService read tools register when the read backend also satisfies the
	// service read slice. The production adapter (passed as kube) does; a
	// cluster-only reader does not, so its server simply omits them — matching the
	// "tools are advertised only when wired" convention (spec §6).
	if svcReader, ok := kube.(domain.ServiceReader); ok {
		addServiceTools(server, domain.NewServiceService(svcReader, cfg.DefaultNamespace))
	}

	// The RayJob read tools (the wedge) register only when their backend is wired.
	// main.go always wires it; a bare server (or a cluster-only test) leaves it zero.
	if wedge.Jobs != nil {
		addJobTools(server, domain.NewJobService(wedge.Jobs, wedge.Reach, wedge.API, cfg.DefaultNamespace))
	}

	if cfg.AllowMutations {
		applySvc := domain.NewApplyService(write, audit)
		writeSvc := domain.NewClusterWriteService(write, kube, write, applySvc, cfg.DefaultNamespace)
		addClusterWriteTools(server, writeSvc, cfg.AllowRawSpec, cfg.AllowDestructive)

		// RayJob writes (Task 18/19) share the one apply pipeline (and thus the one
		// audit sink) with the cluster writes; the adapter is also the JobBaseBuilder,
		// the JobGetter (mode-aware delete reads the live job), and the Deleter.
		jobWriteSvc := domain.NewJobWriteService(write, write, write, applySvc, cfg.DefaultNamespace)
		addJobWriteTools(server, jobWriteSvc, cfg.AllowRawSpec, cfg.AllowDestructive)

		// RayService writes (Task 21/22): deploy + update + delete. The adapter is
		// also the ServiceBaseBuilder and the Deleter; the ServiceGetter (for update's
		// read-modify-apply and delete's guards, the narrow read slice those paths
		// need) is satisfied by the read backend (kube) when it implements it — same
		// "advertise only when wired" gate as the reads. Delete additionally gates on
		// the destructive tier (it cascades to the owned cluster).
		if svcGetter, ok := kube.(domain.ServiceGetter); ok {
			svcWriteSvc := domain.NewServiceWriteService(write, svcGetter, write, applySvc, cfg.DefaultNamespace)
			addServiceWriteTools(server, svcWriteSvc, cfg.AllowRawSpec, cfg.AllowDestructive)
		}
	}

	return server
}
