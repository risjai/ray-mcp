package mcp

import (
	"context"
	"errors"
	"fmt"
	"time"

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

// clusterEventsInput is the ray_cluster_events argument object. Name is required;
// namespace defaults to the server default; limit caps the returned events.
type clusterEventsInput struct {
	Name      string `json:"name"                jsonschema:"the RayCluster name (required)"`
	Namespace string `json:"namespace,omitempty" jsonschema:"namespace of the RayCluster; defaults to the server default namespace"`
	Limit     int    `json:"limit,omitempty"     jsonschema:"max events to return; 0 uses the server default (~25). Warnings are prioritized over Normal events when trimming"`
}

// eventRow is one bounded event in the structured output (spec §10: a relevant
// slice, never the raw firehose). AgeSeconds is the time since LastSeen so the
// agent reads recency without parsing timestamps.
type eventRow struct {
	Type       string `json:"type"`
	Reason     string `json:"reason"`
	Message    string `json:"message"`
	Count      int32  `json:"count"`
	AgeSeconds int64  `json:"ageSeconds"`
}

// ClusterEventsOutput is the structured ray_cluster_events result: the bounded
// event rows plus the count and warning count for the text summary.
type ClusterEventsOutput struct {
	Events   []eventRow `json:"events"   jsonschema:"recent, bounded events for the cluster + its pods, Warnings first then most recent"`
	Count    int        `json:"count"    jsonschema:"the number of events returned (after bounding)"`
	Warnings int        `json:"warnings" jsonschema:"how many of the returned events are Warnings"`
}

// addClusterTools registers ray_cluster_list, ray_cluster_get and
// ray_cluster_events against the domain ClusterService. All are read-only; the
// handlers do validation + mapping only, with all read policy living in the
// service.
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
			return nil, ClusterGetOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toClusterGetOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterGetSummary(out)}},
		}, out, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "ray_cluster_events",
		Description: "Recent, bounded Kubernetes events for a RayCluster — operator/reconcile events on the cluster object merged with scheduler/kubelet events on its pods (FailedScheduling, ErrImagePull, OOMKilled, ...), Warnings first then most recent. A relevant slice, never the raw event firehose. Read-only.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, func(ctx context.Context, _ *mcp.CallToolRequest, in clusterEventsInput) (*mcp.CallToolResult, ClusterEventsOutput, error) {
		if in.Name == "" {
			return nil, ClusterEventsOutput{}, errors.New("name is required")
		}

		res, err := svc.Events(ctx, domain.ClusterEventsRequest{
			Namespace: in.Namespace,
			Name:      in.Name,
			Limit:     in.Limit,
		})
		if err != nil {
			return nil, ClusterEventsOutput{}, mapDomainError(err) //nolint:wrapcheck // mapped to a clean, bounded tool error.
		}

		out := toClusterEventsOutput(res)
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: clusterEventsSummary(res, out)}},
		}, out, nil
	})
}

// toClusterEventsOutput maps the domain events result to the structured output,
// computing the per-event age and the warning count for the summary.
func toClusterEventsOutput(res domain.ClusterEventsResult) ClusterEventsOutput {
	rows := make([]eventRow, 0, len(res.Events))
	warnings := 0
	for _, e := range res.Events {
		if e.Type == "Warning" {
			warnings++
		}
		rows = append(rows, eventRow{
			Type:       e.Type,
			Reason:     e.Reason,
			Message:    e.Message,
			Count:      e.Count,
			AgeSeconds: eventAgeSeconds(e.LastSeen),
		})
	}
	return ClusterEventsOutput{Events: rows, Count: len(rows), Warnings: warnings}
}

// eventAgeSeconds reports the time since an event was last seen, guarding a zero
// timestamp (no resolvable lastSeen) so it never reports a multi-decade age.
func eventAgeSeconds(lastSeen time.Time) int64 {
	if lastSeen.IsZero() {
		return 0
	}
	return int64(time.Since(lastSeen).Seconds())
}

// clusterEventsSummary renders the one-line human-readable text content. An empty
// list must NOT read as "healthy": k8s expires events after ~1h, so absence is
// silence, not a clean bill of health — the text says so explicitly.
func clusterEventsSummary(res domain.ClusterEventsResult, out ClusterEventsOutput) string {
	if out.Count == 0 {
		return fmt.Sprintf(
			"no recent events for %q in namespace %q (Kubernetes expires events after ~1h, so this is not a clean bill of health)",
			res.Name, res.Namespace,
		)
	}
	return fmt.Sprintf(
		"%d recent events for %q in namespace %q (%d warnings)",
		out.Count, res.Name, res.Namespace, out.Warnings,
	)
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
