package kuberay

import (
	"strings"
	"testing"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RayService distillation is a multi-branch rollout ladder (RollingBack >
// RollingOut > Running > Initializing > Unknown), a serve-status derivation off
// the Ready condition, and unhealthy-app detection. That logic earns fast pure
// unit tests over typed in-memory RayService objects (mirroring job_submit_test.go);
// the envtest (service_envtest_test.go) proves the same mapping survives a real
// apiserver round-trip. No operator runs here, so we build conditions directly.

// svcWithConditions builds a typed RayService carrying the given status fields.
func svcWithConditions(name string, numEndpoints int32, conds ...metav1.Condition) *rayv1.RayService {
	return &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: rayv1.RayServiceStatuses{
			Conditions:        conds,
			NumServeEndpoints: numEndpoints,
		},
	}
}

func svcCond(t rayv1.RayServiceConditionType, s metav1.ConditionStatus, reason rayv1.RayServiceConditionReason, msg string) metav1.Condition {
	return metav1.Condition{Type: string(t), Status: s, Reason: string(reason), Message: msg}
}

// TestServiceRolloutPhaseLadder asserts the phase ladder switches on condition
// type/status in priority order (verified against the operator's calculateConditions).
func TestServiceRolloutPhaseLadder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rs   *rayv1.RayService
		want string
	}{
		{
			name: "Ready True -> Running",
			rs:   svcWithConditions("s", 3, svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok")),
			want: "Running",
		},
		{
			name: "UpgradeInProgress True -> RollingOut",
			rs: svcWithConditions("s", 2,
				svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok"),
				svcCond(rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.BothActivePendingClustersExist, "both exist"),
			),
			want: "RollingOut",
		},
		{
			name: "RollbackInProgress outranks UpgradeInProgress -> RollingBack",
			rs: svcWithConditions("s", 2,
				svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok"),
				svcCond(rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.BothActivePendingClustersExist, "both exist"),
				svcCond(rayv1.RollbackInProgress, metav1.ConditionTrue, rayv1.TargetClusterChanged, "rolling back"),
			),
			want: "RollingBack",
		},
		{
			name: "Ready False with Initializing reason -> Initializing",
			rs:   svcWithConditions("s", 0, svcCond(rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceInitializing, "RayService is initializing")),
			want: "Initializing",
		},
		{
			name: "Ready False with InitializingTimeout reason -> Initializing",
			rs:   svcWithConditions("s", 0, svcCond(rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceInitializingTimeout, "timed out")),
			want: "Initializing",
		},
		{
			name: "Ready False with ValidationFailed reason -> Unknown",
			rs:   svcWithConditions("s", 0, svcCond(rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceValidationFailed, "bad serveConfigV2")),
			want: "Unknown",
		},
		{
			name: "no conditions, empty status -> Unknown",
			rs:   svcWithConditions("s", 0),
			want: "Unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := serviceRolloutPhase(tc.rs); got != tc.want {
				t.Errorf("serviceRolloutPhase = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestServiceRolloutPhaseDeprecatedFallback asserts that with no conditions the
// phase falls back to the deprecated ServiceStatus (an older operator that
// populated ServiceStatus but not Conditions).
func TestServiceRolloutPhaseDeprecatedFallback(t *testing.T) {
	t.Parallel()

	rs := svcWithConditions("s", 1)
	rs.Status.ServiceStatus = rayv1.Running //nolint:staticcheck // SA1019: deliberately set the deprecated field to exercise the fallback path.

	if got := serviceRolloutPhase(rs); got != "Running" {
		t.Errorf("serviceRolloutPhase with deprecated ServiceStatus=Running = %q, want Running", got)
	}
}

// TestServiceServeStatus asserts the serve-status column derives from the Ready
// condition (True->Running, False->reason), falling back to the deprecated
// ServiceStatus, then Unknown — never blank.
func TestServiceServeStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rs   *rayv1.RayService
		want string
	}{
		{
			name: "Ready True -> Running",
			rs:   svcWithConditions("s", 3, svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok")),
			want: "Running",
		},
		{
			name: "Ready False -> surfaces the reason",
			rs:   svcWithConditions("s", 0, svcCond(rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceValidationFailed, "bad")),
			want: "ValidationFailed",
		},
		{
			name: "no conditions, empty -> Unknown",
			rs:   svcWithConditions("s", 0),
			want: "Unknown",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := serviceServeStatus(tc.rs); got != tc.want {
				t.Errorf("serviceServeStatus = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestServiceHealthHealthy asserts a steady-state healthy service composes a
// clean phase + endpoints line with no trailing detail.
func TestServiceHealthHealthy(t *testing.T) {
	t.Parallel()

	rs := svcWithConditions("s", 3, svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok"))

	if got := serviceHealth(rs); got != "Running; 3 serve endpoints" {
		t.Errorf("serviceHealth = %q, want %q", got, "Running; 3 serve endpoints")
	}
}

// TestServiceHealthRollingOutUnhealthyNewApp asserts the wedge signal: during a
// rollout the in-flight (pending) cluster's unhealthy apps surface as
// "new serve UNHEALTHY: <apps>" (sorted, both UNHEALTHY and DEPLOY_FAILED named).
func TestServiceHealthRollingOutUnhealthyNewApp(t *testing.T) {
	t.Parallel()

	rs := svcWithConditions("s", 2,
		svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok"),
		svcCond(rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.BothActivePendingClustersExist, "both exist"),
	)
	rs.Status.PendingServiceStatus.Applications = map[string]rayv1.AppStatus{
		"app_b":  {Status: rayv1.ApplicationStatusEnum.UNHEALTHY},
		"app_a":  {Status: rayv1.ApplicationStatusEnum.DEPLOY_FAILED},
		"ok_app": {Status: rayv1.ApplicationStatusEnum.RUNNING},
	}

	got := serviceHealth(rs)
	if !strings.HasPrefix(got, "RollingOut; ") {
		t.Errorf("serviceHealth = %q, want it to start with the RollingOut phase", got)
	}
	if !strings.Contains(got, "new serve UNHEALTHY: app_a, app_b") {
		t.Errorf("serviceHealth = %q, want it to name the unhealthy new apps sorted (app_a, app_b)", got)
	}
}

// TestServiceHealthSteadyStateUnhealthyApp asserts that outside a rollout the
// active cluster's unhealthy apps surface as "serve UNHEALTHY: <app>".
func TestServiceHealthSteadyStateUnhealthyApp(t *testing.T) {
	t.Parallel()

	rs := svcWithConditions("s", 1, svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok"))
	rs.Status.ActiveServiceStatus.Applications = map[string]rayv1.AppStatus{
		"default": {Status: rayv1.ApplicationStatusEnum.UNHEALTHY},
	}

	got := serviceHealth(rs)
	if !strings.Contains(got, "serve UNHEALTHY: default") {
		t.Errorf("serviceHealth = %q, want it to name the unhealthy active app", got)
	}
	if strings.Contains(got, "new serve") {
		t.Errorf("serviceHealth = %q, must not use the \"new serve\" prefix outside a rollout", got)
	}
}

// TestServiceHealthSurfacesNotReadyReason asserts that when not ready and no app
// is named unhealthy, the Ready condition's reason/message surfaces (so a
// ValidationFailed service is actionable from the one-line health).
func TestServiceHealthSurfacesNotReadyReason(t *testing.T) {
	t.Parallel()

	rs := svcWithConditions("s", 0, svcCond(rayv1.RayServiceReady, metav1.ConditionFalse, rayv1.RayServiceValidationFailed, "invalid serveConfigV2"))

	got := serviceHealth(rs)
	if !strings.Contains(got, "ValidationFailed: invalid serveConfigV2") {
		t.Errorf("serviceHealth = %q, want it to surface the ValidationFailed reason:message", got)
	}
}

// TestServiceHealthNeverBlank asserts an empty status still yields a non-blank
// health line (mirrors RayJob's never-blank discipline).
func TestServiceHealthNeverBlank(t *testing.T) {
	t.Parallel()

	if got := serviceHealth(svcWithConditions("s", 0)); got == "" {
		t.Error("serviceHealth on an empty status returned blank, want a non-blank line")
	}
}

// TestToServiceDetailCarriesRaw asserts the detail sets the GVK before
// conversion so the verbose escape hatch hands back apiVersion/kind.
func TestToServiceDetailCarriesRaw(t *testing.T) {
	t.Parallel()

	rs := svcWithConditions("demo", 3, svcCond(rayv1.RayServiceReady, metav1.ConditionTrue, rayv1.NonZeroServeEndpoints, "ok"))

	detail, err := toServiceDetail(rs)
	if err != nil {
		t.Fatalf("toServiceDetail: %v", err)
	}
	if detail.Name != "demo" {
		t.Errorf("Name = %q, want demo", detail.Name)
	}
	if detail.RolloutPhase != "Running" {
		t.Errorf("RolloutPhase = %q, want Running", detail.RolloutPhase)
	}
	if detail.HealthyReplicas != 3 {
		t.Errorf("HealthyReplicas = %d, want 3 (NumServeEndpoints)", detail.HealthyReplicas)
	}
	if detail.Raw == nil {
		t.Fatal("Raw is nil, want the full object map")
	}
	if kind, _ := detail.Raw["kind"].(string); kind != "RayService" {
		t.Errorf("Raw[kind] = %q, want RayService", kind)
	}
}
