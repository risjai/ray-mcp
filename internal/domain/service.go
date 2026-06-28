package domain

import "context"

// ServiceReader is the narrow read slice of KubeRayPort the RayService read tools
// need (ray_service_list / ray_service_get). ServiceService depends on this (not
// the full port) so callers wire only what these tools use, mirroring
// ClusterReader / JobReader. The KubeRay adapter satisfies it; the full
// KubeRayPort satisfies it too.
type ServiceReader interface {
	ListServices(ctx context.Context, namespace string, opts ListOptions) (ServiceList, error)
	GetService(ctx context.Context, namespace, name string) (ServiceDetail, error)
}

// ServiceService is the thin orchestration layer over the KubeRay read path for
// the RayService read tools. Like ClusterService it owns only the cross-cutting
// read policy: the default-namespace fallback, pagination pass-through (the
// adapter applies the 0→50 cap; the domain surfaces the continue token upward),
// and the verbose/distilled Raw gate (spec §10). It imports no Kubernetes or HTTP
// packages — only the port and the DTOs.
type ServiceService struct {
	kube             ServiceReader
	defaultNamespace string
}

// NewServiceService builds the service. The default namespace is injected as a
// plain string (not the config struct) so the domain stays free of any config or
// Kubernetes import.
func NewServiceService(kube ServiceReader, defaultNamespace string) *ServiceService {
	return &ServiceService{kube: kube, defaultNamespace: defaultNamespace}
}

// ServiceListRequest is the decoded ray_service_list argument set. Namespace is
// optional (defaults to the service's default namespace); AllNamespaces overrides
// the namespace scope; Limit/Continue carry the token-economy pagination. Mirrors
// ClusterListRequest.
type ServiceListRequest struct {
	Namespace     string
	Limit         int
	Continue      string
	AllNamespaces bool
}

// ServiceListResult is one page of service summaries plus the honest pagination
// signal. As with ClusterListResult, k8s list returns no total, so MoreAvailable
// is derived purely from the presence of a continue token — never a fabricated
// total.
type ServiceListResult struct {
	Items         []ServiceSummary
	Continue      string // k8s continue token for the next page; empty when exhausted.
	MoreAvailable bool   // true iff Continue != "" — never a fabricated total.
}

// List applies the namespace default + AllNamespaces scope and passes the
// pagination options through to the port (the adapter applies the 0→50 cap). It
// mirrors ClusterService.List: the continue token surfaces unchanged and
// MoreAvailable is the derived "more available vs showing all" signal.
func (s *ServiceService) List(ctx context.Context, req ServiceListRequest) (ServiceListResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	list, err := s.kube.ListServices(ctx, namespace, ListOptions{
		Limit:         req.Limit,
		Continue:      req.Continue,
		AllNamespaces: req.AllNamespaces,
	})
	if err != nil {
		return ServiceListResult{}, err
	}

	return ServiceListResult{
		Items:         list.Items,
		Continue:      list.Continue,
		MoreAvailable: list.Continue != "",
	}, nil
}

// ServiceGetRequest is the decoded ray_service_get argument set. Name is required
// (validated at the MCP edge); Namespace defaults to the service's default;
// Verbose toggles the full-object escape hatch.
type ServiceGetRequest struct {
	Namespace string
	Name      string
	Verbose   bool
}

// ServiceGetResult carries the distilled detail and the verbosity decision. When
// Verbose is false the full Raw object is stripped here in the domain — the
// distilled view must never dump Raw by default (spec §10), and stripping it in
// the service (not the MCP layer) keeps that policy in one place.
type ServiceGetResult struct {
	Detail  ServiceDetail
	Verbose bool
}

// Get fetches one service and applies the verbosity gate. A *NotFoundError from
// the port propagates unchanged for the MCP layer to map to a clean tool error.
func (s *ServiceService) Get(ctx context.Context, req ServiceGetRequest) (ServiceGetResult, error) {
	namespace := s.resolveNamespace(req.Namespace)

	detail, err := s.kube.GetService(ctx, namespace, req.Name)
	if err != nil {
		return ServiceGetResult{}, err
	}

	if !req.Verbose {
		// Distilled by default: drop the full object so it never reaches the agent
		// unless explicitly asked for.
		detail.Raw = nil
	}

	return ServiceGetResult{Detail: detail, Verbose: req.Verbose}, nil
}

// resolveNamespace applies the default-namespace fallback. AllNamespaces is
// handled by the port via ListOptions, so the resolved namespace is only the
// scope for a namespaced list/get.
func (s *ServiceService) resolveNamespace(ns string) string {
	if ns == "" {
		return s.defaultNamespace
	}
	return ns
}
