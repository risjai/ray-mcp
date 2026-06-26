package kuberay

import (
	"testing"

	"github.com/risjai/ray-mcp/internal/domain"
)

// BuildJobBase is pure construction (curated params → typed RayJob → unstructured
// map), so unlike the cluster base it gets a fast no-tags unit test here. The
// envtest job (job_submit_envtest_test.go) proves the shape the apiserver accepts;
// these prove the mode mapping the controller's behavior depends on.

// nestedStr is a tiny path reader for assertions on the unstructured base.
func nestedStr(t *testing.T, m map[string]any, path ...string) string {
	t.Helper()
	var cur any = m
	for _, p := range path {
		cm, ok := cur.(map[string]any)
		if !ok {
			t.Fatalf("path %v: %q is not a map (got %T)", path, p, cur)
		}
		cur = cm[p]
	}
	s, ok := cur.(string)
	if !ok {
		t.Fatalf("path %v: leaf is not a string (got %T: %v)", path, cur, cur)
	}
	return s
}

func nestedMap(t *testing.T, m map[string]any, path ...string) (map[string]any, bool) {
	t.Helper()
	var cur any = m
	for _, p := range path {
		cm, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}
		cur = cm[p]
	}
	out, ok := cur.(map[string]any)
	return out, ok
}

func TestBuildJobBaseExistingClusterSelector(t *testing.T) {
	t.Parallel()
	c := &Client{}
	base, err := c.BuildJobBase(domain.JobSubmitParams{
		Namespace:       "ray",
		Name:            "job1",
		Entrypoint:      "python main.py",
		ExistingCluster: "existing-cluster",
	})
	if err != nil {
		t.Fatalf("BuildJobBase: %v", err)
	}

	if got := nestedStr(t, base, "apiVersion"); got != "ray.io/v1" {
		t.Errorf("apiVersion = %q, want ray.io/v1", got)
	}
	if got := nestedStr(t, base, "kind"); got != "RayJob" {
		t.Errorf("kind = %q, want RayJob", got)
	}
	if got := nestedStr(t, base, "metadata", "name"); got != "job1" {
		t.Errorf("metadata.name = %q, want job1", got)
	}
	if got := nestedStr(t, base, "metadata", "namespace"); got != "ray" {
		t.Errorf("metadata.namespace = %q, want ray", got)
	}
	if got := nestedStr(t, base, "spec", "entrypoint"); got != "python main.py" {
		t.Errorf("spec.entrypoint = %q, want python main.py", got)
	}
	// Existing-cluster mode → clusterSelector["ray.io/cluster"], NO rayClusterSpec.
	if got := nestedStr(t, base, "spec", "clusterSelector", "ray.io/cluster"); got != "existing-cluster" {
		t.Errorf("spec.clusterSelector[ray.io/cluster] = %q, want existing-cluster", got)
	}
	if _, ok := nestedMap(t, base, "spec", "rayClusterSpec"); ok {
		t.Error("spec.rayClusterSpec present in existing-cluster mode, want absent")
	}
}

func TestBuildJobBaseEphemeralRayClusterSpec(t *testing.T) {
	t.Parallel()
	c := &Client{}
	shutdown := true
	base, err := c.BuildJobBase(domain.JobSubmitParams{
		Namespace:  "ray",
		Name:       "job1",
		Entrypoint: "python main.py",
		ClusterSpec: &domain.ClusterSubmitSpec{
			RayVersion:   "2.9.0",
			Image:        "rayproject/ray:2.9.0",
			WorkerGroups: []domain.WorkerGroupParams{{Name: "wg", Replicas: 2}},
		},
		ShutdownAfterJobFinishes: &shutdown,
	})
	if err != nil {
		t.Fatalf("BuildJobBase: %v", err)
	}

	// Ephemeral mode → rayClusterSpec present, NO clusterSelector.
	rcs, ok := nestedMap(t, base, "spec", "rayClusterSpec")
	if !ok {
		t.Fatal("spec.rayClusterSpec absent in ephemeral mode, want present")
	}
	if rcs["rayVersion"] != "2.9.0" {
		t.Errorf("rayClusterSpec.rayVersion = %v, want 2.9.0", rcs["rayVersion"])
	}
	if _, ok := nestedMap(t, base, "spec", "rayClusterSpec", "headGroupSpec"); !ok {
		t.Error("rayClusterSpec.headGroupSpec absent, want a head group")
	}
	if _, ok := nestedMap(t, base, "spec", "clusterSelector"); ok {
		t.Error("spec.clusterSelector present in ephemeral mode, want absent")
	}
	// shutdownAfterJobFinishes carried onto the RayJob spec.
	if got, ok := base["spec"].(map[string]any)["shutdownAfterJobFinishes"]; !ok || got != true {
		t.Errorf("spec.shutdownAfterJobFinishes = %v (present=%t), want true", got, ok)
	}
}

// TestBuildJobBaseEphemeralExplicitShutdownFalse is the regression guard for the
// "pass false to keep the cluster for debugging" contract (Q16b). The typed
// rayv1.RayJob field is `omitempty`, so a concrete false would be DROPPED on
// conversion to unstructured — making the base byte-identical to "unset" and
// leaving the value to KubeRay's incidental default rather than ray-mcp owning it
// via SSA. The adapter must set the key explicitly so an explicit false survives.
func TestBuildJobBaseEphemeralExplicitShutdownFalse(t *testing.T) {
	t.Parallel()
	c := &Client{}
	keep := false
	base, err := c.BuildJobBase(domain.JobSubmitParams{
		Namespace:  "ray",
		Name:       "job1",
		Entrypoint: "python main.py",
		ClusterSpec: &domain.ClusterSubmitSpec{
			RayVersion:   "2.9.0",
			Image:        "rayproject/ray:2.9.0",
			WorkerGroups: []domain.WorkerGroupParams{{Name: "wg", Replicas: 1}},
		},
		ShutdownAfterJobFinishes: &keep,
	})
	if err != nil {
		t.Fatalf("BuildJobBase: %v", err)
	}
	got, ok := base["spec"].(map[string]any)["shutdownAfterJobFinishes"]
	if !ok {
		t.Fatal("spec.shutdownAfterJobFinishes absent for an explicit false; omitempty must not drop it")
	}
	if got != false {
		t.Errorf("spec.shutdownAfterJobFinishes = %v, want explicit false carried", got)
	}
}

func TestBuildJobBaseRuntimeEnvAndMetadata(t *testing.T) {
	t.Parallel()
	c := &Client{}
	base, err := c.BuildJobBase(domain.JobSubmitParams{
		Namespace:       "ray",
		Name:            "job1",
		Entrypoint:      "python main.py",
		ExistingCluster: "c",
		RuntimeEnvYAML:  "pip:\n  - requests\n",
		Metadata:        map[string]string{"team": "ml"},
	})
	if err != nil {
		t.Fatalf("BuildJobBase: %v", err)
	}
	if got := nestedStr(t, base, "spec", "runtimeEnvYAML"); got != "pip:\n  - requests\n" {
		t.Errorf("spec.runtimeEnvYAML = %q, want the YAML doc", got)
	}
	if got := nestedStr(t, base, "spec", "metadata", "team"); got != "ml" {
		t.Errorf("spec.metadata.team = %q, want ml", got)
	}
}
