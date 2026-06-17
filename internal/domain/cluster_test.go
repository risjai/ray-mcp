package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// newClusterFake seeds a fakeKubeRay with the given cluster details keyed by
// namespace/name.
func newClusterFake(details ...ClusterDetail) *fakeKubeRay {
	clusters := make(map[string]ClusterDetail, len(details))
	for _, d := range details {
		clusters[key(d.Namespace, d.Name)] = d
	}
	return &fakeKubeRay{clusters: clusters}
}

// TestListDefaultsNamespace asserts an omitted request namespace falls back to
// the service default and that default is what reaches the port.
func TestListDefaultsNamespace(t *testing.T) {
	t.Parallel()

	fake := newClusterFake(ClusterDetail{
		ClusterSummary: ClusterSummary{Name: "demo", Namespace: "ray-system", Phase: "Ready"},
	})
	svc := NewClusterService(fake, "ray-system")

	res, err := svc.List(context.Background(), ClusterListRequest{Namespace: ""})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].Name != "demo" {
		t.Fatalf("List items = %+v, want exactly the demo cluster", res.Items)
	}
	if fake.lastListOpts.AllNamespaces {
		t.Errorf("AllNamespaces leaked true into the port for a defaulted-namespace list")
	}
}

// TestListExplicitNamespaceOverridesDefault asserts a provided namespace is used
// instead of the default.
func TestListExplicitNamespaceOverridesDefault(t *testing.T) {
	t.Parallel()

	fake := newClusterFake(
		ClusterDetail{ClusterSummary: ClusterSummary{Name: "in-team", Namespace: "team-a"}},
		ClusterDetail{ClusterSummary: ClusterSummary{Name: "in-default", Namespace: "default"}},
	)
	svc := NewClusterService(fake, "default")

	res, err := svc.List(context.Background(), ClusterListRequest{Namespace: "team-a"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].Name != "in-team" {
		t.Fatalf("List items = %+v, want only the team-a cluster", res.Items)
	}
}

// TestListAllNamespacesPassThrough asserts AllNamespaces reaches the port and
// returns clusters across namespaces.
func TestListAllNamespacesPassThrough(t *testing.T) {
	t.Parallel()

	fake := newClusterFake(
		ClusterDetail{ClusterSummary: ClusterSummary{Name: "a", Namespace: "team-a"}},
		ClusterDetail{ClusterSummary: ClusterSummary{Name: "b", Namespace: "team-b"}},
	)
	svc := NewClusterService(fake, "default")

	res, err := svc.List(context.Background(), ClusterListRequest{AllNamespaces: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !fake.lastListOpts.AllNamespaces {
		t.Errorf("AllNamespaces did not pass through to the port; lastListOpts = %+v", fake.lastListOpts)
	}
	if len(res.Items) != 2 {
		t.Fatalf("List returned %d items, want 2 across namespaces", len(res.Items))
	}
}

// TestListMoreAvailableFromContinueToken asserts the "more available vs showing
// all" signal is derived purely from the continue token.
func TestListMoreAvailableFromContinueToken(t *testing.T) {
	t.Parallel()

	t.Run("token present -> more available", func(t *testing.T) {
		t.Parallel()
		fake := newClusterFake(ClusterDetail{ClusterSummary: ClusterSummary{Name: "a", Namespace: "default"}})
		fake.listClustersContinue = "next-page-token"
		svc := NewClusterService(fake, "default")

		res, err := svc.List(context.Background(), ClusterListRequest{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !res.MoreAvailable {
			t.Errorf("MoreAvailable = false, want true (continue token present)")
		}
		if res.Continue != "next-page-token" {
			t.Errorf("Continue = %q, want it surfaced verbatim", res.Continue)
		}
	})

	t.Run("no token -> showing all", func(t *testing.T) {
		t.Parallel()
		fake := newClusterFake(ClusterDetail{ClusterSummary: ClusterSummary{Name: "a", Namespace: "default"}})
		svc := NewClusterService(fake, "default")

		res, err := svc.List(context.Background(), ClusterListRequest{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if res.MoreAvailable {
			t.Errorf("MoreAvailable = true, want false (no continue token)")
		}
		if res.Continue != "" {
			t.Errorf("Continue = %q, want empty", res.Continue)
		}
	})
}

// TestListPassesLimitThrough asserts the limit is passed to the port unchanged
// (the adapter, not the domain, applies the 0→50 default).
func TestListPassesLimitThrough(t *testing.T) {
	t.Parallel()

	fake := newClusterFake()
	svc := NewClusterService(fake, "default")

	if _, err := svc.List(context.Background(), ClusterListRequest{Limit: 7, Continue: "tok"}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if fake.lastListOpts.Limit != 7 {
		t.Errorf("Limit = %d, want 7 passed through", fake.lastListOpts.Limit)
	}
	if fake.lastListOpts.Continue != "tok" {
		t.Errorf("Continue = %q, want %q passed through", fake.lastListOpts.Continue, "tok")
	}
}

// TestGetDistilledByDefault asserts the default Get strips Raw so the full
// object never reaches the agent unless verbose is requested.
func TestGetDistilledByDefault(t *testing.T) {
	t.Parallel()

	fake := newClusterFake(ClusterDetail{
		ClusterSummary:  ClusterSummary{Name: "demo", Namespace: "default", Phase: "Ready"},
		HeadServiceName: "demo-head-svc",
		DashboardURL:    "http://demo-head-svc.default.svc:8265",
		Raw:             MergedSpec{"kind": "RayCluster"},
	})
	svc := NewClusterService(fake, "default")

	res, err := svc.Get(context.Background(), ClusterGetRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if res.Verbose {
		t.Errorf("Verbose = true, want false by default")
	}
	if res.Detail.Raw != nil {
		t.Errorf("Raw = %+v, want nil (distilled by default)", res.Detail.Raw)
	}
	// The distilled fields must survive.
	if res.Detail.HeadServiceName != "demo-head-svc" {
		t.Errorf("HeadServiceName = %q, want demo-head-svc", res.Detail.HeadServiceName)
	}
	if res.Detail.DashboardURL == "" {
		t.Errorf("DashboardURL empty, want the distilled URL preserved")
	}
}

// TestGetVerboseIncludesRaw asserts verbose returns the full Raw object.
func TestGetVerboseIncludesRaw(t *testing.T) {
	t.Parallel()

	fake := newClusterFake(ClusterDetail{
		ClusterSummary: ClusterSummary{Name: "demo", Namespace: "default"},
		Raw:            MergedSpec{"kind": "RayCluster", "apiVersion": "ray.io/v1"},
	})
	svc := NewClusterService(fake, "default")

	res, err := svc.Get(context.Background(), ClusterGetRequest{Name: "demo", Verbose: true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !res.Verbose {
		t.Errorf("Verbose = false, want true")
	}
	if res.Detail.Raw == nil {
		t.Fatalf("Raw = nil, want the full object under verbose")
	}
	if kind, _ := res.Detail.Raw["kind"].(string); kind != "RayCluster" {
		t.Errorf("Raw[kind] = %q, want RayCluster", kind)
	}
}

// TestGetNotFoundPropagates asserts a *NotFoundError from the port propagates
// unchanged (the MCP layer maps it to a clean tool error).
func TestGetNotFoundPropagates(t *testing.T) {
	t.Parallel()

	svc := NewClusterService(newClusterFake(), "default")

	_, err := svc.Get(context.Background(), ClusterGetRequest{Name: "missing"})
	if err == nil {
		t.Fatal("Get on a missing cluster returned nil error, want NotFoundError")
	}
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *NotFoundError", err, err)
	}
	if nf.Kind != KindRayCluster || nf.Name != "missing" || nf.Namespace != "default" {
		t.Errorf("NotFoundError = %+v, want RayCluster missing in default", nf)
	}
}

// TestEventsDefaultsNamespaceAndLimit asserts the service applies the
// default-namespace fallback and the limit default when both are omitted, and
// returns the seeded events for that resolved key.
func TestEventsDefaultsNamespaceAndLimit(t *testing.T) {
	t.Parallel()

	fake := &fakeKubeRay{events: map[string][]Event{
		key("ray-system", "demo"): {
			{Type: "Warning", Reason: "FailedScheduling", LastSeen: time.Now()},
		},
	}}
	svc := NewClusterService(fake, "ray-system")

	res, err := svc.Events(context.Background(), ClusterEventsRequest{Name: "demo"})
	if err != nil {
		t.Fatalf("Events: %v", err)
	}
	if fake.lastEventsNamespace != "ray-system" {
		t.Errorf("namespace reaching the port = %q, want the default ray-system", fake.lastEventsNamespace)
	}
	if fake.lastEventsLimit != defaultEventLimit {
		t.Errorf("limit reaching the port = %d, want the default %d", fake.lastEventsLimit, defaultEventLimit)
	}
	if res.Namespace != "ray-system" || res.Name != "demo" {
		t.Errorf("result scope = %q/%q, want ray-system/demo", res.Namespace, res.Name)
	}
	if len(res.Events) != 1 || res.Events[0].Reason != "FailedScheduling" {
		t.Errorf("Events = %+v, want the one seeded FailedScheduling event", res.Events)
	}
}

// TestEventsExplicitNamespaceAndLimitPassThrough asserts a provided namespace +
// limit reach the port unchanged (the adapter owns the firehose bounding).
func TestEventsExplicitNamespaceAndLimitPassThrough(t *testing.T) {
	t.Parallel()

	fake := &fakeKubeRay{events: map[string][]Event{}}
	svc := NewClusterService(fake, "default")

	if _, err := svc.Events(context.Background(), ClusterEventsRequest{
		Namespace: "team-a", Name: "demo", Limit: 5,
	}); err != nil {
		t.Fatalf("Events: %v", err)
	}
	if fake.lastEventsNamespace != "team-a" {
		t.Errorf("namespace = %q, want team-a passed through", fake.lastEventsNamespace)
	}
	if fake.lastEventsLimit != 5 {
		t.Errorf("limit = %d, want 5 passed through", fake.lastEventsLimit)
	}
}

// TestEventsEmptyIsNotAnError asserts an empty event slice is a valid result, not
// an error — k8s expires events after ~1h, so absence is silence, not failure.
func TestEventsEmptyIsNotAnError(t *testing.T) {
	t.Parallel()

	svc := NewClusterService(&fakeKubeRay{events: map[string][]Event{}}, "default")

	res, err := svc.Events(context.Background(), ClusterEventsRequest{Name: "quiet"})
	if err != nil {
		t.Fatalf("Events on a cluster with no events errored: %v", err)
	}
	if len(res.Events) != 0 {
		t.Errorf("Events = %+v, want empty", res.Events)
	}
}
