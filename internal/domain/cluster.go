package domain

import "context"

// ClusterReader is the narrow read slice of KubeRayPort the cluster read tools
// need. ClusterService depends on this (not the full port) so callers wire only
// what these tools use — the KubeRay adapter satisfies it with just its cluster
// read methods implemented, and the full KubeRayPort satisfies it too. It folds
// in the event read so the three RayCluster read tools (list/get/events) share
// one narrow dependency.
type ClusterReader interface {
	ListClusters(ctx context.Context, namespace string, opts ListOptions) (ClusterList, error)
	GetCluster(ctx context.Context, namespace, name string) (ClusterDetail, error)
	Events(ctx context.Context, kind Kind, namespace, name string, limit int) ([]Event, error)
}

// ClusterService is the thin orchestration layer over the KubeRay read path for
// the RayCluster read tools (ray_cluster_list / ray_cluster_get). It owns the
// cross-cutting read policy the MCP layer must not duplicate: the
// default-namespace fallback, pagination cap pass-through (the adapter defaults
// 0→50; the domain's job is to surface the continue token upward, never silently
// drop), and the verbose/distilled gate (spec §10). It imports no Kubernetes or
// HTTP packages — only the port and the DTOs.
type ClusterService struct {
	kube             ClusterReader
	defaultNamespace string
}

// NewClusterService builds the service. The default namespace is injected as a
// plain string (not the config struct) so the domain stays free of any config
// or Kubernetes import.
func NewClusterService(kube ClusterReader, defaultNamespace string) *ClusterService {
	return &ClusterService{kube: kube, defaultNamespace: defaultNamespace}
}

// ClusterListRequest is the decoded ray_cluster_list argument set. Namespace is
// optional (defaults to the service's default namespace); AllNamespaces overrides
// the namespace scope; Limit/Continue carry the token-economy pagination.
type ClusterListRequest struct {
	Namespace     string
	Limit         int
	Continue      string
	AllNamespaces bool
}

// ClusterListResult is one page of cluster summaries plus the honest pagination
// signal. k8s list does NOT return a total count, so the result never implies
// one: MoreAvailable is derived purely from the presence of a continue token.
// The MCP layer renders "showing N; more available (continue=…)" when
// MoreAvailable is true, or "showing all N" when it is false.
type ClusterListResult struct {
	Items         []ClusterSummary
	Continue      string // k8s continue token for the next page; empty when exhausted.
	MoreAvailable bool   // true iff Continue != "" — never a fabricated total.
}

// List applies the namespace default + AllNamespaces scope and passes the
// pagination options through to the port. The continue token surfaces unchanged
// so the caller can fetch the next page; MoreAvailable is the derived "more
// available vs showing all" signal.
func (s *ClusterService) List(ctx context.Context, req ClusterListRequest) (ClusterListResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	list, err := s.kube.ListClusters(ctx, namespace, ListOptions{
		Limit:         req.Limit,
		Continue:      req.Continue,
		AllNamespaces: req.AllNamespaces,
	})
	if err != nil {
		return ClusterListResult{}, err
	}

	return ClusterListResult{
		Items:         list.Items,
		Continue:      list.Continue,
		MoreAvailable: list.Continue != "",
	}, nil
}

// ClusterGetRequest is the decoded ray_cluster_get argument set. Name is
// required (validated at the MCP edge); Namespace defaults to the service's
// default; Verbose toggles the full-object escape hatch.
type ClusterGetRequest struct {
	Namespace string
	Name      string
	Verbose   bool
}

// ClusterGetResult carries the distilled detail and the verbosity decision. When
// Verbose is false the full Raw object is stripped here in the domain — the
// distilled view must never dump Raw by default (spec §10), and stripping it in
// the service (not the MCP layer) keeps that policy in one place.
type ClusterGetResult struct {
	Detail  ClusterDetail
	Verbose bool
}

// Get fetches one cluster and applies the verbosity gate. A *NotFoundError from
// the port propagates unchanged for the MCP layer to map to a clean tool error.
func (s *ClusterService) Get(ctx context.Context, req ClusterGetRequest) (ClusterGetResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	detail, err := s.kube.GetCluster(ctx, namespace, req.Name)
	if err != nil {
		return ClusterGetResult{}, err
	}

	if !req.Verbose {
		// Distilled by default: drop the full object so it never reaches the agent
		// unless explicitly asked for.
		detail.Raw = nil
	}

	return ClusterGetResult{Detail: detail, Verbose: req.Verbose}, nil
}

// defaultEventLimit is the namespace-level default the service passes through
// when the caller omits a limit. The adapter applies its own (smaller) default
// when it still sees 0, so this is just the domain-side documented intent; the
// service stays thin and lets the adapter own the firehose bounding policy.
const defaultEventLimit = 25

// ClusterEventsRequest is the decoded ray_cluster_events argument set. Name is
// required (validated at the MCP edge); Namespace defaults to the service's
// default; Limit caps the returned events (0 → defaultEventLimit).
type ClusterEventsRequest struct {
	Namespace string
	Name      string
	Limit     int
}

// ClusterEventsResult carries the bounded event slice plus the resolved scope so
// the MCP layer can render an honest summary ("N events for X in NS"). An empty
// Events slice is valid (and common — k8s expires events after ~1h); the MCP
// layer must not read it as "healthy".
type ClusterEventsResult struct {
	Namespace string
	Name      string
	Events    []Event
}

// Events fetches recent, bounded k8s events for a RayCluster (operator events on
// the object + scheduler/kubelet events on its pods, merged and bounded by the
// adapter). The service owns only the cross-cutting policy: the default-namespace
// fallback and the limit default; the adapter owns the gather/merge/Warning-first
// bounding. A *NotFoundError-style error from the port propagates unchanged.
func (s *ClusterService) Events(ctx context.Context, req ClusterEventsRequest) (ClusterEventsResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	limit := req.Limit
	if limit <= 0 {
		limit = defaultEventLimit
	}

	events, err := s.kube.Events(ctx, KindRayCluster, namespace, req.Name, limit)
	if err != nil {
		return ClusterEventsResult{}, err
	}

	return ClusterEventsResult{Namespace: namespace, Name: req.Name, Events: events}, nil
}

// resolveNamespace applies the default-namespace fallback. AllNamespaces is
// handled by the port via ListOptions, so the resolved namespace is only the
// scope for a namespaced list/get.
func (s *ClusterService) resolveNamespace(ns string) string {
	if ns == "" {
		return s.defaultNamespace
	}
	return ns
}
