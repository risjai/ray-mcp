package mcp

import (
	"context"
	"fmt"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/version"
)

// capabilitiesInput is the (empty) argument object for ray_capabilities. The
// tool takes no parameters; an empty struct infers an empty object schema.
type capabilitiesInput struct{}

// CapabilitiesOutput is the structured result of ray_capabilities. It is the
// StructuredContent of the CallToolResult; every field is config-derived, with
// no live Kubernetes call (Task 4 scope + the Task 9 static field-set report).
type CapabilitiesOutput struct {
	ServerVersion    string          `json:"serverVersion"    jsonschema:"the ray-mcp server version"`
	KubeContext      string          `json:"kubeContext"      jsonschema:"the bound kubeconfig context name"`
	DefaultNamespace string          `json:"defaultNamespace" jsonschema:"the namespace used when a tool omits one"`
	EnabledTiers     []string        `json:"enabledTiers"     jsonschema:"the enabled tool tiers (read, write, destructive)"`
	KubeRayTested    string          `json:"kubeRayTested"    jsonschema:"the CI-tested KubeRay version range"`
	FieldValidation  fieldValidation `json:"fieldValidation"  jsonschema:"how create/update inputs are validated, and the curated field set per CRD"`
}

// fieldValidation reports how the apply pipeline validates inputs and what the
// curated field set is per CRD (the Task 4 deferral, re-homed here per Decision
// Gate 1 / B2). B2 DEMOTED the boot-time CRD-schema-read: there is no live schema
// introspection and no get-CRD RBAC grant. Instead, every apply runs an
// unconditional server-side DryRunAll (Q5) — that is the validation/pruning oracle
// — and this report lists the STATIC curated field set ray-mcp exposes per CRD.
// An unknown field reaches the API server via rawSpec and is REJECTED there (a
// structural-schema CRD rejects undeclared fields; it does not silently prune).
type fieldValidation struct {
	// Mode is always "server-side-dry-run": the validation oracle is the API
	// server's DryRunAll on every apply, not a client-side schema read.
	Mode string `json:"mode" jsonschema:"the validation strategy: server-side-dry-run (every apply is DryRunAll-validated)"`
	// PruningDetection reports whether unknown fields are caught. Under SSA against
	// a structural CRD they are REJECTED (hard error), so this is always true — but
	// it is reported as rejection, never silent pruning.
	PruningDetection bool `json:"pruningDetection" jsonschema:"true: unknown fields are rejected by the server-side dry-run (not silently pruned)"`
	// CuratedFields lists the curated parameter names ray-mcp exposes per CRD kind.
	// Anything outside this set is reachable only via the rawSpec escape hatch
	// (when --allow-raw-spec is set) and validated server-side.
	CuratedFields map[string][]string `json:"curatedFields" jsonschema:"the static curated parameter names exposed per CRD kind; richer fields go through rawSpec"`
}

// curatedClusterFields is the static curated field set for RayCluster create
// (spec §6, Gate 1 C3 "curated params stay thin"). It is reported by
// ray_capabilities so an agent can see, without trial and error, which knobs are
// first-class vs which need the rawSpec escape hatch.
var curatedClusterFields = []string{
	"name", "namespace", "rayVersion", "image",
	"headResources{cpu,memory,gpu}",
	"workerGroups[]{name,replicas,minReplicas,maxReplicas,resources}",
	"enableAutoscaling", "labels", "annotations", "rawSpec",
}

// enabledTiers derives the enabled tool tiers from config. It is pure (no I/O)
// so it can be unit-tested directly. The read tier is always on; write requires
// --allow-mutations; destructive ADDITIONALLY requires --allow-mutations (spec §6:
// "destructive tools additionally require --allow-destructive"). So
// --allow-destructive without --allow-mutations is inert — there is no write tier
// for it to extend — and is not reported, matching the registration gate (the
// destructive tools are never registered without mutations either).
func enabledTiers(cfg *config.Config) []string {
	tiers := []string{"read"}
	if cfg.AllowMutations {
		tiers = append(tiers, "write")
		if cfg.AllowDestructive {
			tiers = append(tiers, "destructive")
		}
	}
	return tiers
}

// capabilitiesSource supplies the cluster-binding fields ray_capabilities
// reports. The kuberay skeleton adapter satisfies it config-only (no dialing).
type capabilitiesSource interface {
	ContextName() string
	DefaultNamespace() string
}

// addCapabilitiesTool registers the read-only ray_capabilities meta tool on the
// server. The handler builds its result purely from cfg and the source — no
// Kubernetes API call — and returns BOTH the typed structured output and a short
// human-readable text summary.
func addCapabilitiesTool(server *mcp.Server, cfg *config.Config, src capabilitiesSource) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_capabilities",
		Description: "Report ray-mcp server version, bound kubeconfig context, default namespace, enabled tool tiers, and the CI-tested KubeRay version range. Read-only; makes no live Kubernetes call.",
	}, func(_ context.Context, _ *mcp.CallToolRequest, _ capabilitiesInput) (*mcp.CallToolResult, CapabilitiesOutput, error) {
		out := CapabilitiesOutput{
			ServerVersion:    version.Version,
			KubeContext:      src.ContextName(),
			DefaultNamespace: src.DefaultNamespace(),
			EnabledTiers:     enabledTiers(cfg),
			KubeRayTested:    version.KubeRayTested,
			FieldValidation: fieldValidation{
				Mode:             "server-side-dry-run",
				PruningDetection: true,
				CuratedFields:    map[string][]string{"RayCluster": curatedClusterFields},
			},
		}
		// Set Content explicitly to a short summary; if left unset, the SDK
		// would auto-fill Content with the JSON dump of the output, not a
		// human-readable line. StructuredContent is auto-populated from out.
		result := &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: capabilitiesSummary(out)}},
		}
		return result, out, nil
	})
}

// capabilitiesSummary renders the one-line human-readable text content.
func capabilitiesSummary(out CapabilitiesOutput) string {
	return fmt.Sprintf(
		"ray-mcp %s | context %s | default-namespace %s | tiers: %s | KubeRay tested %s",
		out.ServerVersion,
		out.KubeContext,
		out.DefaultNamespace,
		strings.Join(out.EnabledTiers, ","),
		out.KubeRayTested,
	)
}
