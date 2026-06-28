package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// serviceListInput is the ray_service_list argument object. All fields are
// optional; an omitted namespace falls back to the server default, and the
// pagination fields carry the token economy (spec §10). Mirrors clusterListInput.
type serviceListInput struct {
	Namespace     string `json:"namespace,omitempty"     jsonschema:"namespace to list RayServices in; defaults to the server default namespace"`
	Limit         int    `json:"limit,omitempty"         jsonschema:"max rows per page; 0 uses the server default (~50)"`
	Continue      string `json:"continue,omitempty"      jsonschema:"opaque continue token from a prior page; omit for the first page"`
	AllNamespaces bool   `json:"allNamespaces,omitempty" jsonschema:"list across all namespaces instead of one"`
}

// serviceRow is one compact list row (spec §10: tiny rows — name, namespace,
// serve status, healthy serve endpoints, age, 1-line health — never full status).
type serviceRow struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ServiceStatus   string `json:"serviceStatus"`
	HealthyReplicas int32  `json:"healthyReplicas"`
	AgeSeconds      int64  `json:"ageSeconds"`
	Health          string `json:"health"`
}

// ServiceListOutput is the structured ray_service_list result: the compact rows
// plus the honest pagination signal. As with ClusterListOutput there is NO total
// count — k8s does not return one — so MoreAvailable + Continue is the entire
// pagination contract.
type ServiceListOutput struct {
	Services      []serviceRow `json:"services"      jsonschema:"the compact service rows for this page"`
	Count         int          `json:"count"         jsonschema:"the number of rows shown on this page"`
	MoreAvailable bool         `json:"moreAvailable" jsonschema:"true if more pages exist; pass continue for the next page"`
	Continue      string       `json:"continue,omitempty" jsonschema:"the continue token for the next page; empty when exhausted"`
}

// serviceGetInput is the ray_service_get argument object. Name is required;
// namespace defaults to the server default; verbose toggles the full-object
// escape hatch.
type serviceGetInput struct {
	Name      string `json:"name"                jsonschema:"the RayService name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayService; defaults to the server default namespace"`
	Verbose   bool   `json:"verbose,omitempty"   jsonschema:"include the full unstructured object under raw; default is the distilled view"`
}

// ServiceGetOutput is the structured ray_service_get result: the distilled detail
// always (serve status, healthy serve endpoints, rollout phase, age, health), plus
// Raw only when verbose was requested (spec §10).
type ServiceGetOutput struct {
	Name            string         `json:"name"`
	Namespace       string         `json:"namespace"`
	ServiceStatus   string         `json:"serviceStatus"`
	HealthyReplicas int32          `json:"healthyReplicas"`
	RolloutPhase    string         `json:"rolloutPhase"`
	AgeSeconds      int64          `json:"ageSeconds"`
	Health          string         `json:"health"`
	Raw             map[string]any `json:"raw,omitempty" jsonschema:"the full unstructured object; present only when verbose=true"`
}

// addServiceTools registers ray_service_list and ray_service_get against the
// domain ServiceService. Both are read-only; the handlers do validation + mapping
// only, with all read policy living in the service (mirrors addClusterTools).
func addServiceTools(server *mcp.Server, svc *domain.ServiceService) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_service_list",
		Description: "List RayServices as compact rows (name, namespace, serve status, healthy serve endpoints, age, 1-line health), capped and paginated via an opaque continue token. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in serviceListInput) (*mcp.CallToolResult, ServiceListOutput, error) {
		res, err := svc.List(ctx, domain.ServiceListRequest{
			Namespace:     in.Namespace,
			Limit:         in.Limit,
			Continue:      in.Continue,
			AllNamespaces: in.AllNamespaces,
		})
		if err != nil {
			return nil, ServiceListOutput{}, err //nolint:wrapcheck // surfaced as a clean tool error; domain errors carry their own actionable message.
		}

		out := toServiceListOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: serviceListSummary(in, res)}},
		}, out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_service_get",
		Description: "Get one RayService's distilled status (serve status, healthy serve endpoints, rollout phase, age, health). Pass verbose=true for the full unstructured object. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in serviceGetInput) (*mcp.CallToolResult, ServiceGetOutput, error) {
		if in.Name == "" {
			return nil, ServiceGetOutput{}, errors.New("name is required")
		}

		res, err := svc.Get(ctx, domain.ServiceGetRequest{
			Namespace: in.Namespace,
			Name:      in.Name,
			Verbose:   in.Verbose,
		})
		if err != nil {
			return nil, ServiceGetOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toServiceGetOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: serviceGetSummary(out)}},
		}, out, nil
	})
}

// toServiceListOutput maps the domain list result to the structured output.
func toServiceListOutput(res domain.ServiceListResult) ServiceListOutput {
	rows := make([]serviceRow, 0, len(res.Items))
	for _, s := range res.Items {
		rows = append(rows, serviceRow{
			Name:            s.Name,
			Namespace:       s.Namespace,
			ServiceStatus:   s.ServiceStatus,
			HealthyReplicas: s.HealthyReplicas,
			AgeSeconds:      int64(s.Age.Seconds()),
			Health:          s.Health,
		})
	}
	return ServiceListOutput{
		Services:      rows,
		Count:         len(rows),
		MoreAvailable: res.MoreAvailable,
		Continue:      res.Continue,
	}
}

// toServiceGetOutput maps the domain get result to the structured output. Raw is
// carried only when verbose (the service has already stripped it otherwise).
func toServiceGetOutput(res domain.ServiceGetResult) ServiceGetOutput {
	d := res.Detail
	out := ServiceGetOutput{
		Name:            d.Name,
		Namespace:       d.Namespace,
		ServiceStatus:   d.ServiceStatus,
		HealthyReplicas: d.HealthyReplicas,
		RolloutPhase:    d.RolloutPhase,
		AgeSeconds:      int64(d.Age.Seconds()),
		Health:          d.Health,
	}
	if res.Verbose && d.Raw != nil {
		out.Raw = d.Raw
	}
	return out
}

// serviceListSummary renders the one-line human-readable text content. It is
// honest about pagination: "more available" only when a continue token is
// present, "showing all" otherwise — never an implied total k8s did not give
// (mirrors clusterListSummary).
func serviceListSummary(in serviceListInput, res domain.ServiceListResult) string {
	scope := fmt.Sprintf("namespace %q", in.Namespace)
	if in.AllNamespaces {
		scope = "all namespaces"
	} else if in.Namespace == "" {
		scope = "the default namespace"
	}

	n := len(res.Items)
	if res.MoreAvailable {
		return fmt.Sprintf(
			"%d RayServices in %s (showing %d; more available — pass continue=%q for the next page)",
			n, scope, n, res.Continue,
		)
	}
	return fmt.Sprintf("%d RayServices in %s (showing all %d)", n, scope, n)
}

// serviceGetSummary renders the one-line human-readable text content for a get.
func serviceGetSummary(out ServiceGetOutput) string {
	return fmt.Sprintf(
		"RayService %q in namespace %q: %s",
		out.Name, out.Namespace, out.Health,
	)
}
