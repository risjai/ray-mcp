package domain

import (
	"context"
	"errors"
	"testing"
)

// newServiceFake seeds a fakeKubeRay with the given service details keyed by
// namespace/name.
func newServiceFake(details ...ServiceDetail) *fakeKubeRay {
	services := make(map[string]ServiceDetail, len(details))
	for _, d := range details {
		services[key(d.Namespace, d.Name)] = d
	}
	return &fakeKubeRay{services: services}
}

// TestServiceListDefaultsNamespace asserts an omitted request namespace falls
// back to the service default and that default is what reaches the port.
func TestServiceListDefaultsNamespace(t *testing.T) {
	t.Parallel()

	fake := newServiceFake(ServiceDetail{
		ServiceSummary: ServiceSummary{Name: "demo", Namespace: "ray-system", ServiceStatus: "Running"},
	})
	svc := NewServiceService(fake, "ray-system")

	res, err := svc.List(context.Background(), ServiceListRequest{Namespace: ""})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].Name != "demo" {
		t.Fatalf("List items = %+v, want exactly the demo service", res.Items)
	}
	if fake.lastListOpts.AllNamespaces {
		t.Errorf("AllNamespaces leaked true into the port for a defaulted-namespace list")
	}
}

// TestServiceListExplicitNamespaceOverridesDefault asserts a provided namespace
// is used instead of the default.
func TestServiceListExplicitNamespaceOverridesDefault(t *testing.T) {
	t.Parallel()

	fake := newServiceFake(
		ServiceDetail{ServiceSummary: ServiceSummary{Name: "in-team", Namespace: "team-a"}},
		ServiceDetail{ServiceSummary: ServiceSummary{Name: "in-default", Namespace: "default"}},
	)
	svc := NewServiceService(fake, "default")

	res, err := svc.List(context.Background(), ServiceListRequest{Namespace: "team-a"})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].Name != "in-team" {
		t.Fatalf("List items = %+v, want only the team-a service", res.Items)
	}
}

// TestServiceListAllNamespacesPassThrough asserts AllNamespaces reaches the port
// and returns services across namespaces.
func TestServiceListAllNamespacesPassThrough(t *testing.T) {
	t.Parallel()

	fake := newServiceFake(
		ServiceDetail{ServiceSummary: ServiceSummary{Name: "a", Namespace: "team-a"}},
		ServiceDetail{ServiceSummary: ServiceSummary{Name: "b", Namespace: "team-b"}},
	)
	svc := NewServiceService(fake, "default")

	res, err := svc.List(context.Background(), ServiceListRequest{AllNamespaces: true})
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

// TestServiceListMoreAvailableFromContinueToken asserts the "more available vs
// showing all" signal is derived purely from the continue token.
func TestServiceListMoreAvailableFromContinueToken(t *testing.T) {
	t.Parallel()

	t.Run("token present -> more available", func(t *testing.T) {
		t.Parallel()
		fake := newServiceFake(ServiceDetail{ServiceSummary: ServiceSummary{Name: "a", Namespace: "default"}})
		fake.listServicesContinue = "next-page-token"
		svc := NewServiceService(fake, "default")

		res, err := svc.List(context.Background(), ServiceListRequest{})
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
		fake := newServiceFake(ServiceDetail{ServiceSummary: ServiceSummary{Name: "a", Namespace: "default"}})
		svc := NewServiceService(fake, "default")

		res, err := svc.List(context.Background(), ServiceListRequest{})
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

// TestServiceListPassesLimitThrough asserts the limit + continue are passed to
// the port unchanged (the adapter, not the domain, applies the 0→50 default).
func TestServiceListPassesLimitThrough(t *testing.T) {
	t.Parallel()

	fake := newServiceFake()
	svc := NewServiceService(fake, "default")

	if _, err := svc.List(context.Background(), ServiceListRequest{Limit: 7, Continue: "tok"}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if fake.lastListOpts.Limit != 7 {
		t.Errorf("Limit = %d, want 7 passed through", fake.lastListOpts.Limit)
	}
	if fake.lastListOpts.Continue != "tok" {
		t.Errorf("Continue = %q, want %q passed through", fake.lastListOpts.Continue, "tok")
	}
}

// TestServiceGetDistilledByDefault asserts the default Get strips Raw so the full
// object never reaches the agent unless verbose is requested.
func TestServiceGetDistilledByDefault(t *testing.T) {
	t.Parallel()

	fake := newServiceFake(ServiceDetail{
		ServiceSummary: ServiceSummary{Name: "demo", Namespace: "default", ServiceStatus: "Running"},
		RolloutPhase:   "Running",
		Raw:            MergedSpec{"kind": "RayService"},
	})
	svc := NewServiceService(fake, "default")

	res, err := svc.Get(context.Background(), ServiceGetRequest{Name: "demo"})
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
	if res.Detail.RolloutPhase != "Running" {
		t.Errorf("RolloutPhase = %q, want Running", res.Detail.RolloutPhase)
	}
	if res.Detail.ServiceStatus != "Running" {
		t.Errorf("ServiceStatus = %q, want the distilled status preserved", res.Detail.ServiceStatus)
	}
}

// TestServiceGetVerboseIncludesRaw asserts verbose returns the full Raw object.
func TestServiceGetVerboseIncludesRaw(t *testing.T) {
	t.Parallel()

	fake := newServiceFake(ServiceDetail{
		ServiceSummary: ServiceSummary{Name: "demo", Namespace: "default"},
		Raw:            MergedSpec{"kind": "RayService", "apiVersion": "ray.io/v1"},
	})
	svc := NewServiceService(fake, "default")

	res, err := svc.Get(context.Background(), ServiceGetRequest{Name: "demo", Verbose: true})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !res.Verbose {
		t.Errorf("Verbose = false, want true")
	}
	if res.Detail.Raw == nil {
		t.Fatalf("Raw = nil, want the full object under verbose")
	}
	if kind, _ := res.Detail.Raw["kind"].(string); kind != "RayService" {
		t.Errorf("Raw[kind] = %q, want RayService", kind)
	}
}

// TestServiceGetNotFoundPropagates asserts a *NotFoundError from the port
// propagates unchanged (the MCP layer maps it to a clean tool error).
func TestServiceGetNotFoundPropagates(t *testing.T) {
	t.Parallel()

	svc := NewServiceService(newServiceFake(), "default")

	_, err := svc.Get(context.Background(), ServiceGetRequest{Name: "missing"})
	if err == nil {
		t.Fatal("Get on a missing service returned nil error, want NotFoundError")
	}
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *NotFoundError", err, err)
	}
	if nf.Kind != KindRayService || nf.Name != "missing" || nf.Namespace != "default" {
		t.Errorf("NotFoundError = %+v, want RayService missing in default", nf)
	}
}
