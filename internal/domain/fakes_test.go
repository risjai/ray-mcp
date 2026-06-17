package domain

import (
	"context"
	"testing"
)

// In-memory fakes for the three ports. They live in a _test.go file so they are
// never compiled into the production binary, and in package domain (same-package
// test) so the compile-time satisfaction asserts below can reference the
// unexported-free interfaces directly.

// Compile-time proof that the fakes satisfy the ports — the real proof the
// contract is implementable. The RayAPI fake can ONLY implement the two read
// methods; there is no write method to implement, which is how the read-only
// invariant is structurally enforced.
var (
	_ KubeRayPort     = (*fakeKubeRay)(nil)
	_ RayAPIPort      = (*fakeRayAPI)(nil)
	_ RayReachability = (*fakeReachability)(nil)
)

// fakeKubeRay backs the guarded CRD path with simple maps keyed by
// namespace/name. Tests seed the maps directly. Apply records the last applied
// spec; Delete removes from the cluster map.
type fakeKubeRay struct {
	clusters map[string]ClusterDetail
	jobs     map[string]JobDetail
	services map[string]ServiceDetail
	events   map[string][]Event

	applied MergedSpec // last spec passed to Apply.

	// listClustersContinue, when set, is returned verbatim as the ClusterList
	// continue token so a test can drive the "more available" signal without a
	// real paginating backend. lastListOpts captures the options the service
	// passed through (to assert namespace defaulting / allNamespaces).
	listClustersContinue string
	lastListOpts         ListOptions

	// lastEventsNamespace / lastEventsLimit capture the args the service passed
	// to Events, so a test can assert the namespace default + limit default.
	lastEventsNamespace string
	lastEventsLimit     int
}

func key(namespace, name string) string { return namespace + "/" + name }

func (f *fakeKubeRay) ListClusters(_ context.Context, namespace string, opts ListOptions) (ClusterList, error) {
	f.lastListOpts = opts

	var items []ClusterSummary
	for _, c := range f.clusters {
		if opts.AllNamespaces || c.Namespace == namespace {
			items = append(items, c.ClusterSummary)
		}
	}

	return ClusterList{Items: items, Continue: f.listClustersContinue}, nil
}

func (f *fakeKubeRay) GetCluster(_ context.Context, namespace, name string) (ClusterDetail, error) {
	c, ok := f.clusters[key(namespace, name)]
	if !ok {
		return ClusterDetail{}, &NotFoundError{Kind: KindRayCluster, Namespace: namespace, Name: name}
	}

	return c, nil
}

func (f *fakeKubeRay) ListJobs(_ context.Context, namespace string, _ ListOptions) (JobList, error) {
	var items []JobSummary
	for _, j := range f.jobs {
		if j.Namespace == namespace {
			items = append(items, j.JobSummary)
		}
	}

	return JobList{Items: items, Continue: ""}, nil
}

func (f *fakeKubeRay) GetJob(_ context.Context, namespace, name string) (JobDetail, error) {
	j, ok := f.jobs[key(namespace, name)]
	if !ok {
		return JobDetail{}, &NotFoundError{Kind: KindRayJob, Namespace: namespace, Name: name}
	}

	return j, nil
}

func (f *fakeKubeRay) ListServices(_ context.Context, namespace string, _ ListOptions) (ServiceList, error) {
	var items []ServiceSummary
	for _, s := range f.services {
		if s.Namespace == namespace {
			items = append(items, s.ServiceSummary)
		}
	}

	return ServiceList{Items: items, Continue: ""}, nil
}

func (f *fakeKubeRay) GetService(_ context.Context, namespace, name string) (ServiceDetail, error) {
	s, ok := f.services[key(namespace, name)]
	if !ok {
		return ServiceDetail{}, &NotFoundError{Kind: KindRayService, Namespace: namespace, Name: name}
	}

	return s, nil
}

func (f *fakeKubeRay) Apply(_ context.Context, _ Kind, _, _ string, spec MergedSpec, _ bool) (MergedSpec, error) {
	f.applied = spec

	return spec, nil
}

func (f *fakeKubeRay) Delete(_ context.Context, _ Kind, namespace, name string, _ bool) error {
	delete(f.clusters, key(namespace, name))

	return nil
}

func (f *fakeKubeRay) Events(_ context.Context, _ Kind, namespace, name string, limit int) ([]Event, error) {
	f.lastEventsNamespace = namespace
	f.lastEventsLimit = limit
	return f.events[key(namespace, name)], nil
}

// fakeRayAPI backs the read-only wedge with maps keyed by submission id. It can
// only implement the two read methods — there is nothing else to implement.
type fakeRayAPI struct {
	status map[string]RayJobStatus
	logs   map[string]RayJobLogs
}

func (f *fakeRayAPI) JobStatus(_ context.Context, _ Endpoint, jobID string) (RayJobStatus, error) {
	s, ok := f.status[jobID]
	if !ok {
		return RayJobStatus{}, &NotFoundError{Kind: KindRayJob, Name: jobID}
	}

	return s, nil
}

func (f *fakeRayAPI) JobLogs(_ context.Context, _ Endpoint, jobID string, _ LogOptions) (RayJobLogs, error) {
	l, ok := f.logs[jobID]
	if !ok {
		return RayJobLogs{}, &NotFoundError{Kind: KindRayJob, Name: jobID}
	}

	return l, nil
}

// fakeReachability returns a canned endpoint regardless of strategy.
type fakeReachability struct {
	endpoint Endpoint
}

func (f *fakeReachability) Endpoint(_ context.Context, _, _ string, _ int) (Endpoint, error) {
	return f.endpoint, nil
}

func TestFakesSatisfyPorts(t *testing.T) {
	ctx := t.Context()

	kr := &fakeKubeRay{
		clusters: map[string]ClusterDetail{
			key("ray", "demo"): {
				ClusterSummary: ClusterSummary{Name: "demo", Namespace: "ray", Phase: "ready"},
			},
		},
	}

	got, err := kr.GetCluster(ctx, "ray", "demo")
	if err != nil {
		t.Fatalf("GetCluster: unexpected error: %v", err)
	}

	if got.Name != "demo" || got.Phase != "ready" {
		t.Fatalf("GetCluster: round-trip mismatch: got %+v", got.ClusterSummary)
	}

	api := &fakeRayAPI{
		status: map[string]RayJobStatus{
			"raysubmit_1": {JobID: "raysubmit_1", Status: "RUNNING"},
		},
	}

	st, err := api.JobStatus(ctx, Endpoint{BaseURL: "http://head:8265"}, "raysubmit_1")
	if err != nil {
		t.Fatalf("JobStatus: unexpected error: %v", err)
	}

	if st.Status != "RUNNING" {
		t.Fatalf("JobStatus: round-trip mismatch: got %q, want %q", st.Status, "RUNNING")
	}

	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://head:8265"}}

	ep, err := reach.Endpoint(ctx, "ray", "demo", 8265)
	if err != nil {
		t.Fatalf("Endpoint: unexpected error: %v", err)
	}

	if ep.BaseURL != "http://head:8265" {
		t.Fatalf("Endpoint: round-trip mismatch: got %q", ep.BaseURL)
	}
}
