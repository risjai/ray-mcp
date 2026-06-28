//go:build envtest

// Tier 2 (envtest) coverage for the KubeRay adapter's RayService read path (Task
// 20). It boots the same real apiserver+etcd+CRD harness as envtest_test.go and
// exercises GetService/ListServices' status→ServiceDetail distillation end-to-end:
// the rollout phase ladder, the serve-status derivation off the Ready condition,
// and HealthyReplicas = NumServeEndpoints — proven against a real apiserver.
//
// As with the cluster/job tests, NO operator runs in envtest, so .status is
// written directly via the /status subresource. Conditions are written with the
// shared cond() helper (envtest_test.go), which stamps LastTransitionTime — the
// apiserver schema-validates metav1.Condition and rejects a zero transition time.
package kuberay

import (
	"context"
	"errors"
	"testing"
	"time"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/risjai/ray-mcp/internal/domain"
)

// newRayService builds a minimal valid RayService. The CRD requires
// spec.rayClusterConfig (an embedded RayClusterSpec with head + worker groups), so
// the spec must satisfy it to pass envtest's schema validation on create.
func newRayService(namespace, name string) *rayv1.RayService {
	headContainer := corev1.Container{Name: "ray-head", Image: "rayproject/ray:2.9.0"}
	workerContainer := corev1.Container{Name: "ray-worker", Image: "rayproject/ray:2.9.0"}

	return &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: rayv1.RayServiceSpec{
			RayClusterSpec: rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					RayStartParams: map[string]string{},
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{headContainer}},
					},
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
					GroupName:      "workers",
					Replicas:       ptr[int32](1),
					MinReplicas:    ptr[int32](0),
					MaxReplicas:    ptr[int32](5),
					RayStartParams: map[string]string{},
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{Containers: []corev1.Container{workerContainer}},
					},
				}},
			},
		},
	}
}

// TestGetServiceMapsRunningStatus creates a RayService, writes a Ready status
// (Ready=True + NumServeEndpoints) via the /status subresource, and asserts
// GetService distills it into a Running, healthy ServiceDetail.
func TestGetServiceMapsRunningStatus(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "running-service"
	)

	rs := newRayService(namespace, name)
	if err := k8s.Create(ctx, rs); err != nil {
		t.Fatalf("create RayService: %v", err)
	}

	rs.Status = rayv1.RayServiceStatuses{
		NumServeEndpoints: 3,
		ServiceStatus:     rayv1.Running,
		Conditions: []metav1.Condition{
			cond(string(rayv1.RayServiceReady), metav1.ConditionTrue, string(rayv1.NonZeroServeEndpoints), "Number of serve endpoints is greater than 0"),
		},
	}
	if err := k8s.Status().Update(ctx, rs); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	detail, err := adapter.GetService(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}

	if detail.Name != name {
		t.Errorf("Name = %q, want %q", detail.Name, name)
	}
	if detail.RolloutPhase != "Running" {
		t.Errorf("RolloutPhase = %q, want Running", detail.RolloutPhase)
	}
	if detail.ServiceStatus != "Running" {
		t.Errorf("ServiceStatus = %q, want Running", detail.ServiceStatus)
	}
	if detail.HealthyReplicas != 3 {
		t.Errorf("HealthyReplicas = %d, want 3 (NumServeEndpoints)", detail.HealthyReplicas)
	}
	if detail.Health != "Running; 3 serve endpoints" {
		t.Errorf("Health = %q, want %q", detail.Health, "Running; 3 serve endpoints")
	}
	if detail.Age <= 0 {
		t.Errorf("Age = %v, want > 0", detail.Age)
	}
	if detail.Raw == nil {
		t.Error("Raw is nil, want the full object map")
	} else if kind, _ := detail.Raw["kind"].(string); kind != "RayService" {
		t.Errorf("Raw[kind] = %q, want RayService", kind)
	}
}

// TestGetServiceRollingOutUnhealthyNewApp writes a mid-rollout status
// (UpgradeInProgress=True with a pending cluster whose new serve app is UNHEALTHY)
// and asserts the wedge signal surfaces: RolloutPhase RollingOut and the health
// line names the unhealthy new app — proven through a real apiserver round-trip.
func TestGetServiceRollingOutUnhealthyNewApp(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "rolling-service"
	)

	rs := newRayService(namespace, name)
	if err := k8s.Create(ctx, rs); err != nil {
		t.Fatalf("create RayService: %v", err)
	}

	rs.Status = rayv1.RayServiceStatuses{
		NumServeEndpoints: 2,
		Conditions: []metav1.Condition{
			cond(string(rayv1.RayServiceReady), metav1.ConditionTrue, string(rayv1.NonZeroServeEndpoints), "ok"),
			cond(string(rayv1.UpgradeInProgress), metav1.ConditionTrue, string(rayv1.BothActivePendingClustersExist), "Both active and pending Ray clusters exist"),
		},
		ActiveServiceStatus: rayv1.RayServiceStatus{
			RayClusterName: "rolling-service-active",
		},
		PendingServiceStatus: rayv1.RayServiceStatus{
			RayClusterName: "rolling-service-pending",
			Applications: map[string]rayv1.AppStatus{
				"default": {Status: rayv1.ApplicationStatusEnum.UNHEALTHY, Message: "deploy failed"},
			},
		},
	}
	if err := k8s.Status().Update(ctx, rs); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	detail, err := adapter.GetService(ctx, namespace, name)
	if err != nil {
		t.Fatalf("GetService: %v", err)
	}

	if detail.RolloutPhase != "RollingOut" {
		t.Errorf("RolloutPhase = %q, want RollingOut", detail.RolloutPhase)
	}
	want := "RollingOut; 2 serve endpoints; new serve UNHEALTHY: default"
	if detail.Health != want {
		t.Errorf("Health = %q, want %q", detail.Health, want)
	}
}

// TestGetServiceNotFound asserts a missing name maps to *domain.NotFoundError
// with the RayService kind.
func TestGetServiceNotFound(t *testing.T) {
	adapter, _ := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, err := adapter.GetService(ctx, "default", "no-such-service")
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *domain.NotFoundError", err, err)
	}
	if nf.Kind != domain.KindRayService {
		t.Errorf("NotFoundError.Kind = %q, want %q", nf.Kind, domain.KindRayService)
	}
	if nf.Name != "no-such-service" {
		t.Errorf("NotFoundError.Name = %q, want %q", nf.Name, "no-such-service")
	}
}

// TestListServicesRoundTrips creates a RayService with a serve status and asserts
// the list row surfaces the distilled serve status + healthy replica count.
func TestListServicesRoundTrips(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const (
		namespace = "default"
		name      = "list-service-one"
	)

	rs := newRayService(namespace, name)
	if err := k8s.Create(ctx, rs); err != nil {
		t.Fatalf("create RayService: %v", err)
	}
	rs.Status = rayv1.RayServiceStatuses{
		NumServeEndpoints: 2,
		ServiceStatus:     rayv1.Running,
		Conditions: []metav1.Condition{
			cond(string(rayv1.RayServiceReady), metav1.ConditionTrue, string(rayv1.NonZeroServeEndpoints), "ok"),
		},
	}
	if err := k8s.Status().Update(ctx, rs); err != nil {
		t.Fatalf("status subresource update: %v", err)
	}

	list, err := adapter.ListServices(ctx, namespace, domain.ListOptions{})
	if err != nil {
		t.Fatalf("ListServices: %v", err)
	}

	got := findServiceSummary(list.Items, name)
	if got == nil {
		t.Fatalf("ListServices did not return %q; items = %+v", name, list.Items)
	}
	if got.ServiceStatus != "Running" {
		t.Errorf("summary ServiceStatus = %q, want Running", got.ServiceStatus)
	}
	if got.HealthyReplicas != 2 {
		t.Errorf("summary HealthyReplicas = %d, want 2", got.HealthyReplicas)
	}
}

// TestServiceServiceEndToEnd drives the DOMAIN ServiceService through the real
// envtest-backed adapter (the adapter satisfies domain.ServiceReader). It proves
// the full read path: namespace defaulting, the distilled-vs-verbose Raw gate, the
// "more available vs showing all" continue signal, and NotFound propagation — all
// against a real apiserver, mirroring TestClusterServiceEndToEnd.
func TestServiceServiceEndToEnd(t *testing.T) {
	adapter, k8s := startAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const namespace = "default"
	names := []string{"svc-a", "svc-b", "svc-c"}
	for _, name := range names {
		if err := k8s.Create(ctx, newRayService(namespace, name)); err != nil {
			t.Fatalf("create %q: %v", name, err)
		}
	}

	svc := domain.NewServiceService(adapter, namespace)

	all, err := svc.List(ctx, domain.ServiceListRequest{})
	if err != nil {
		t.Fatalf("List (default namespace): %v", err)
	}
	if len(all.Items) != len(names) {
		t.Fatalf("List returned %d services, want %d", len(all.Items), len(names))
	}
	if all.MoreAvailable {
		t.Errorf("MoreAvailable = true with no page cap, want false (showing all)")
	}

	page, err := svc.List(ctx, domain.ServiceListRequest{Limit: 1})
	if err != nil {
		t.Fatalf("List (Limit=1): %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("List(Limit=1) returned %d items, want 1", len(page.Items))
	}
	if !page.MoreAvailable || page.Continue == "" {
		t.Errorf("List(Limit=1) MoreAvailable=%v Continue=%q, want true + a token", page.MoreAvailable, page.Continue)
	}

	distilled, err := svc.Get(ctx, domain.ServiceGetRequest{Name: "svc-a"})
	if err != nil {
		t.Fatalf("Get (distilled): %v", err)
	}
	if distilled.Detail.Raw != nil {
		t.Errorf("distilled Get Raw = %+v, want nil", distilled.Detail.Raw)
	}
	if distilled.Detail.Name != "svc-a" {
		t.Errorf("distilled Get Name = %q, want svc-a", distilled.Detail.Name)
	}

	verbose, err := svc.Get(ctx, domain.ServiceGetRequest{Name: "svc-a", Verbose: true})
	if err != nil {
		t.Fatalf("Get (verbose): %v", err)
	}
	if verbose.Detail.Raw == nil {
		t.Fatalf("verbose Get Raw = nil, want the full object")
	}
	if kind, _ := verbose.Detail.Raw["kind"].(string); kind != "RayService" {
		t.Errorf("verbose Get Raw[kind] = %q, want RayService", kind)
	}

	_, err = svc.Get(ctx, domain.ServiceGetRequest{Name: "does-not-exist"})
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Get(missing) error = %T (%v), want *domain.NotFoundError", err, err)
	}
}

// findServiceSummary returns the summary with the given name, or nil.
func findServiceSummary(items []domain.ServiceSummary, name string) *domain.ServiceSummary {
	for i := range items {
		if items[i].Name == name {
			return &items[i]
		}
	}
	return nil
}
