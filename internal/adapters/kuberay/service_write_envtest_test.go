//go:build envtest

// Tier 2 (envtest) coverage for the RayService WRITE paths (Task 21): the curated
// service base builder (BuildServiceBase) composed with the full domain
// ServiceWriteService (Merge + ApplyService) against a real kube-apiserver + etcd +
// KubeRay CRDs. It proves end-to-end, against the INSTALLED CRD schema:
//   - a curated deploy persists a valid RayService the apiserver accepts (with the
//     rayClusterConfig JSON key present);
//   - dryRun validates but persists nothing;
//   - a serve-config-only update returns "in-place" predicted path.
//
// There is NO KubeRay operator here, so .status is never reconciled — these tests
// assert on what the apiserver owns (spec persistence, schema validation).
package kuberay

import (
	"context"
	"errors"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"

	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/observability"
)

// newServiceWriteService wires the real envtest-backed adapter into the full domain
// service-write pipeline (ServiceBaseBuilder + ServiceGetter + ApplyService), so a
// deploy/update exercises curated→base→merge→DryRunAll→SSA→diff→audit and the
// update path reads the live object before the apply (the classifier).
func newServiceWriteService(t *testing.T, adapter *Client) *domain.ServiceWriteService {
	t.Helper()
	apply := domain.NewApplyService(adapter, observability.NewAuditLogger(discardWriter{}))
	// The adapter is also the Deleter (kind-generic Delete) for the destructive tier.
	return domain.NewServiceWriteService(adapter, adapter, adapter, apply, "default")
}

// getRayService fetches a RayService directly (bypassing the adapter) so a test can
// assert what was actually persisted, or that nothing was.
func getRayService(ctx context.Context, t *testing.T, k8s client.Client, namespace, name string) (*rayv1.RayService, error) {
	t.Helper()
	var rs rayv1.RayService
	err := k8s.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, &rs)
	return &rs, err
}

// TestServiceDeployPersists is the deploy AC: a curated deploy persists a
// CRD-valid RayService with rayClusterConfig key present.
func TestServiceDeployPersists(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newServiceWriteService(t, adapter)
	const namespace, name = "default", "deploy-svc"

	res, err := svc.Deploy(ctx, domain.ServiceDeployParams{
		Namespace:     namespace,
		Name:          name,
		ServeConfigV2: "applications:\n  - name: app1\n    import_path: app:app",
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 1, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
	})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if res.DryRun {
		t.Error("result DryRun = true, want false for a committed deploy")
	}
	if res.Name != name || res.Namespace != namespace {
		t.Errorf("result identity = %s/%s, want %s/%s", res.Namespace, res.Name, namespace, name)
	}

	rs, getErr := getRayService(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("deployed service not gettable: %v", getErr)
	}
	if rs.Spec.ServeConfigV2 == "" {
		t.Error("persisted serveConfigV2 is empty, want the deployed serve config")
	}
	if rs.Spec.RayClusterSpec.RayVersion != "2.9.0" {
		t.Errorf("rayClusterConfig.rayVersion = %q, want 2.9.0", rs.Spec.RayClusterSpec.RayVersion)
	}
	if len(rs.Spec.RayClusterSpec.WorkerGroupSpecs) != 1 {
		t.Errorf("worker groups = %d, want 1", len(rs.Spec.RayClusterSpec.WorkerGroupSpecs))
	}
}

// TestServiceDeployDryRunPersistsNothing asserts dryRun=true validates against the
// CRD schema but persists nothing.
func TestServiceDeployDryRunPersistsNothing(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newServiceWriteService(t, adapter)
	const namespace, name = "default", "dryrun-svc"

	res, err := svc.Deploy(ctx, domain.ServiceDeployParams{
		Namespace:     namespace,
		Name:          name,
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 1, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("Deploy(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if _, getErr := getRayService(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("after dry-run the service exists (get err = %v), want NotFound", getErr)
	}
}

// TestServiceUpdateServeConfigInPlace asserts a serve-config-only update on an
// existing service succeeds and returns the "in-place" predicted path.
func TestServiceUpdateServeConfigInPlace(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newServiceWriteService(t, adapter)
	const namespace, name = "default", "update-svc"

	// First deploy a service.
	if _, err := svc.Deploy(ctx, domain.ServiceDeployParams{
		Namespace:     namespace,
		Name:          name,
		ServeConfigV2: "old-config",
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 1, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
	}); err != nil {
		t.Fatalf("initial Deploy: %v", err)
	}

	// Now update only serveConfigV2.
	newCfg := "new-serve-config"
	res, err := svc.Update(ctx, domain.ServiceUpdateParams{
		Namespace:     namespace,
		Name:          name,
		ServeConfigV2: &newCfg,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res.PredictedPath != "in-place" {
		t.Errorf("PredictedPath = %q, want in-place", res.PredictedPath)
	}

	// Verify the persisted service has the new serve config.
	rs, getErr := getRayService(ctx, t, k8s, namespace, name)
	if getErr != nil {
		t.Fatalf("updated service not gettable: %v", getErr)
	}
	if rs.Spec.ServeConfigV2 != "new-serve-config" {
		t.Errorf("persisted serveConfigV2 = %q, want new-serve-config", rs.Spec.ServeConfigV2)
	}
}

// TestServiceDeleteTwoStep is the delete AC against a real apiserver: a preview
// (no confirm) leaves the service in place and yields a fingerprint; the matching
// confirm removes it. There is no operator here, so .status.numServeEndpoints is
// never set — the serving guard does not trigger, exercising the plain confirm path.
func TestServiceDeleteTwoStep(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newServiceWriteService(t, adapter)
	const namespace, name = "default", "delete-svc"

	if _, err := svc.Deploy(ctx, domain.ServiceDeployParams{
		Namespace:     namespace,
		Name:          name,
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 1, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
	}); err != nil {
		t.Fatalf("initial Deploy: %v", err)
	}

	// Preview: no confirm → ConfirmRequiredError, service still present.
	err := svc.Delete(ctx, domain.ServiceDeleteParams{Namespace: namespace, Name: name})
	var required *domain.ConfirmRequiredError
	if !errors.As(err, &required) {
		t.Fatalf("Delete(preview) error = %v, want *ConfirmRequiredError", err)
	}
	if _, getErr := getRayService(ctx, t, k8s, namespace, name); getErr != nil {
		t.Fatalf("service gone after preview (get err = %v), want still present", getErr)
	}

	// Commit: echo the fingerprint → deleted.
	if err := svc.Delete(ctx, domain.ServiceDeleteParams{
		Namespace: namespace, Name: name, Confirm: required.Fingerprint,
	}); err != nil {
		t.Fatalf("Delete(commit): %v", err)
	}
	if _, getErr := getRayService(ctx, t, k8s, namespace, name); !apierrors.IsNotFound(getErr) {
		t.Fatalf("service still present after commit (get err = %v), want NotFound", getErr)
	}
}

// TestServiceDeleteProtectedRefused asserts a service carrying ray-mcp/protected=true
// refuses deletion even with a would-be-valid confirm, and stays present.
func TestServiceDeleteProtectedRefused(t *testing.T) {
	adapter, k8s := startAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	svc := newServiceWriteService(t, adapter)
	const namespace, name = "default", "protected-svc"

	if _, err := svc.Deploy(ctx, domain.ServiceDeployParams{
		Namespace:     namespace,
		Name:          name,
		RayVersion:    "2.9.0",
		Image:         "rayproject/ray:2.9.0",
		HeadResources: domain.ResourceQuantities{CPU: "1", Memory: "2Gi"},
		Annotations:   map[string]string{domain.ProtectedAnnotation: "true"},
		WorkerGroups: []domain.WorkerGroupParams{{
			Name: "workers", Replicas: 1, MinReplicas: 0, MaxReplicas: 5,
			Resources: domain.ResourceQuantities{CPU: "500m", Memory: "1Gi"},
		}},
	}); err != nil {
		t.Fatalf("initial Deploy: %v", err)
	}

	err := svc.Delete(ctx, domain.ServiceDeleteParams{Namespace: namespace, Name: name})
	if err == nil {
		t.Fatal("Delete on protected service returned nil, want refusal")
	}
	var required *domain.ConfirmRequiredError
	if errors.As(err, &required) {
		t.Fatal("protected service yielded a fingerprint; the protected check must precede confirm")
	}
	if _, getErr := getRayService(ctx, t, k8s, namespace, name); getErr != nil {
		t.Fatalf("protected service gone (get err = %v), want still present", getErr)
	}
}

// compile-time proof the production adapter satisfies the service-write backend.
var _ domain.ServiceBaseBuilder = (*Client)(nil)
