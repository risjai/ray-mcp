package mcp

import (
	"context"
	"errors"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/domain"
)

// clusterListInput is the ray_cluster_list argument object. All fields are
// optional; an omitted namespace falls back to the server default, and the
// pagination fields carry the token economy (spec §10).
type clusterListInput struct {
	Namespace     string `json:"namespace,omitempty"     jsonschema:"namespace to list RayClusters in; defaults to the server default namespace"`
	Limit         int    `json:"limit,omitempty"         jsonschema:"max rows per page; 0 uses the server default (~50)"`
	Continue      string `json:"continue,omitempty"      jsonschema:"opaque continue token from a prior page; omit for the first page"`
	AllNamespaces bool   `json:"allNamespaces,omitempty" jsonschema:"list across all namespaces instead of one"`
}

// clusterRow is one compact list row (spec §10: tiny rows — name, namespace,
// phase, ready/desired, age, 1-line health — never full status).
type clusterRow struct {
	Name       string `json:"name"`
	Namespace  string `json:"namespace"`
	Phase      string `json:"phase"`
	Ready      int32  `json:"ready"`
	Desired    int32  `json:"desired"`
	AgeSeconds int64  `json:"ageSeconds"`
	Health     string `json:"health"`
}

// ClusterListOutput is the structured ray_cluster_list result: the compact rows
// plus the honest pagination signal. There is NO total count — k8s does not
// return one — so MoreAvailable + Continue is the entire pagination contract.
type ClusterListOutput struct {
	Clusters      []clusterRow `json:"clusters"      jsonschema:"the compact cluster rows for this page"`
	Count         int          `json:"count"         jsonschema:"the number of rows shown on this page"`
	MoreAvailable bool         `json:"moreAvailable" jsonschema:"true if more pages exist; pass continue for the next page"`
	Continue      string       `json:"continue,omitempty" jsonschema:"the continue token for the next page; empty when exhausted"`
}

// clusterGetInput is the ray_cluster_get argument object. Name is required;
// namespace defaults to the server default; verbose toggles the full-object
// escape hatch.
type clusterGetInput struct {
	Name      string `json:"name"                jsonschema:"the RayCluster name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayCluster; defaults to the server default namespace"`
	Verbose   bool   `json:"verbose,omitempty"   jsonschema:"include the full unstructured object under raw; default is the distilled view"`
}

// ClusterGetOutput is the structured ray_cluster_get result: the distilled
// detail always, plus Raw only when verbose was requested (spec §10).
type ClusterGetOutput struct {
	Name            string         `json:"name"`
	Namespace       string         `json:"namespace"`
	Phase           string         `json:"phase"`
	Ready           int32          `json:"ready"`
	Desired         int32          `json:"desired"`
	AgeSeconds      int64          `json:"ageSeconds"`
	Health          string         `json:"health"`
	HeadServiceName string         `json:"headServiceName,omitempty"`
	DashboardURL    string         `json:"dashboardURL,omitempty"`
	Raw             map[string]any `json:"raw,omitempty" jsonschema:"the full unstructured object; present only when verbose=true"`
}

// addClusterTools registers ray_cluster_list and ray_cluster_get against the
// domain ClusterService. Both are read-only; the handlers do validation +
// mapping only, with all read policy living in the service.
func addClusterTools(server *mcp.Server, svc *domain.ClusterService) {
	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_cluster_list",
		Description: "List RayClusters as compact rows (name, namespace, phase, ready/desired workers, age, 1-line health), capped and paginated via an opaque continue token. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterListInput) (*mcp.CallToolResult, ClusterListOutput, error) {
		res, err := svc.List(ctx, domain.ClusterListRequest{
			Namespace:     in.Namespace,
			Limit:         in.Limit,
			Continue:      in.Continue,
			AllNamespaces: in.AllNamespaces,
		})
		if err != nil {
			return nil, ClusterListOutput{}, err //nolint:wrapcheck // surfaced as a clean tool error; domain errors carry their own actionable message.
		}

		out := toClusterListOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterListSummary(in, res)}},
		}, out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_cluster_get",
		Description: "Get one RayCluster's distilled status (phase, ready/desired workers, age, health, head service, dashboard URL). Pass verbose=true for the full unstructured object. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterGetInput) (*mcp.CallToolResult, ClusterGetOutput, error) {
		if in.Name == "" {
			return nil, ClusterGetOutput{}, errors.New("name is required")
		}

		res, err := svc.Get(ctx, domain.ClusterGetRequest{
			Namespace: in.Namespace,
			Name:      in.Name,
			Verbose:   in.Verbose,
		})
		if err != nil {
			return nil, ClusterGetOutput{}, mapClusterGetError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toClusterGetOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterGetSummary(out)}},
		}, out, nil
	})
}

// toClusterListOutput maps the domain list result to the structured output.
func toClusterListOutput(res domain.ClusterListResult) ClusterListOutput {
	rows := make([]clusterRow, 0, len(res.Items))
	for _, c := range res.Items {
		rows = append(rows, clusterRow{
			Name:       c.Name,
			Namespace:  c.Namespace,
			Phase:      c.Phase,
			Ready:      c.ReadyReplicas,
			Desired:    c.DesiredReplicas,
			AgeSeconds: int64(c.Age.Seconds()),
			Health:     c.Health,
		})
	}
	return ClusterListOutput{
		Clusters:      rows,
		Count:         len(rows),
		MoreAvailable: res.MoreAvailable,
		Continue:      res.Continue,
	}
}

// toClusterGetOutput maps the domain get result to the structured output. Raw is
// carried only when verbose (the service has already stripped it otherwise).
func toClusterGetOutput(res domain.ClusterGetResult) ClusterGetOutput {
	d := res.Detail
	out := ClusterGetOutput{
		Name:            d.Name,
		Namespace:       d.Namespace,
		Phase:           d.Phase,
		Ready:           d.ReadyReplicas,
		Desired:         d.DesiredReplicas,
		AgeSeconds:      int64(d.Age.Seconds()),
		Health:          d.Health,
		HeadServiceName: d.HeadServiceName,
		DashboardURL:    d.DashboardURL,
	}
	if res.Verbose && d.Raw != nil {
		out.Raw = d.Raw
	}
	return out
}

// clusterListSummary renders the one-line human-readable text content. It is
// honest about pagination: "more available" only when a continue token is
// present, "showing all" otherwise — never an implied total k8s did not give.
func clusterListSummary(in clusterListInput, res domain.ClusterListResult) string {
	scope := fmt.Sprintf("namespace %q", in.Namespace)
	if in.AllNamespaces {
		scope = "all namespaces"
	} else if in.Namespace == "" {
		scope = "the default namespace"
	}

	n := len(res.Items)
	if res.MoreAvailable {
		return fmt.Sprintf(
			"%d RayClusters in %s (showing %d; more available — pass continue=%q for the next page)",
			n, scope, n, res.Continue,
		)
	}
	return fmt.Sprintf("%d RayClusters in %s (showing all %d)", n, scope, n)
}

// clusterGetSummary renders the one-line human-readable text content for a get.
func clusterGetSummary(out ClusterGetOutput) string {
	return fmt.Sprintf(
		"RayCluster %q in namespace %q: %s",
		out.Name, out.Namespace, out.Health,
	)
}

// mapClusterGetError maps a domain error to a clean, bounded MCP tool error. A
// *NotFoundError becomes its actionable message rather than a raw dump; other
// errors surface their own message verbatim (they are already bounded by the
// adapter's error taxonomy).
func mapClusterGetError(err error) error {
	var nf *domain.NotFoundError
	if errors.As(err, &nf) {
		return errors.New(nf.Error())
	}
	return err
}
