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
// no live Kubernetes call (Task 4 scope).
type CapabilitiesOutput struct {
	ServerVersion    string   `json:"serverVersion"    jsonschema:"the ray-mcp server version"`
	KubeContext      string   `json:"kubeContext"      jsonschema:"the bound kubeconfig context name"`
	DefaultNamespace string   `json:"defaultNamespace" jsonschema:"the namespace used when a tool omits one"`
	EnabledTiers     []string `json:"enabledTiers"     jsonschema:"the enabled tool tiers (read, write, destructive)"`
	KubeRayTested    string   `json:"kubeRayTested"    jsonschema:"the CI-tested KubeRay version range"`
}

// enabledTiers derives the enabled tool tiers from config. It is pure (no I/O)
// so it can be unit-tested directly. The read tier is always on; write requires
// --allow-mutations; destructive requires --allow-destructive (which itself is
// only meaningful alongside mutations, but tier reporting mirrors the raw flags
// — guard composition is enforced elsewhere).
func enabledTiers(cfg *config.Config) []string {
	tiers := []string{"read"}
	if cfg.AllowMutations {
		tiers = append(tiers, "write")
	}
	if cfg.AllowDestructive {
		tiers = append(tiers, "destructive")
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
