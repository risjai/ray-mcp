//go:build e2e

// Tier 5 (e2e) smoke test. Unlike the hermetic tiers, this runs against a real
// kind cluster with the real KubeRay operator installed (see `make e2e-up`). It
// proves the e2e *infrastructure* is sound before later cluster-touching tasks
// (5, 9, 16a, ...) add their own -tags e2e tests on top:
//   - the kind kubeconfig loads and the real apiserver is reachable
//   - the three KubeRay CRDs (RayCluster/RayJob/RayService) are registered/served
//   - the KubeRay operator deployment exists and is available
//
// It does NOT submit jobs or exercise the wedge — that is each later task's job.
//
// This file is COMPILE-CHECKED but not run as part of Task 4.5 (it needs Docker +
// kind, which the harness build does not have). Run it via `make e2e` (up -> test
// -> down) on a machine with Docker.
//
// TODO(task-5/9): assert ray_capabilities reports the served KubeRay version once
// that is implemented. Task 4 locked ray_capabilities as config-only (no cluster
// call) and served-version reporting is deferred to Task 5/9, so it is NOT
// asserted here despite the testing-strategy §3 prose.
package e2e

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/client/config"
)

// e2eClient builds a controller-runtime client against whatever kubeconfig the
// environment points at (the kind cluster created by `make e2e-up`). The scheme
// carries the built-in types (Deployment) plus apiextensions/v1 (CRDs).
func e2eClient(t *testing.T) client.Client {
	t.Helper()

	cfg, err := ctrlconfig.GetConfig()
	if err != nil {
		t.Fatalf("load kubeconfig (is the kind cluster up? run `make e2e-up`): %v", err)
	}

	sch := runtime.NewScheme()
	if err := scheme.AddToScheme(sch); err != nil {
		t.Fatalf("register client-go scheme: %v", err)
	}
	if err := apiextv1.AddToScheme(sch); err != nil {
		t.Fatalf("register apiextensions scheme: %v", err)
	}

	c, err := client.New(cfg, client.Options{Scheme: sch})
	if err != nil {
		t.Fatalf("build controller-runtime client against the real apiserver: %v", err)
	}
	return c
}

// TestKubeRayCRDsRegistered asserts the real apiserver serves the three KubeRay
// CRDs the operator install registers.
func TestKubeRayCRDsRegistered(t *testing.T) {
	c := e2eClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wantCRDs := []string{
		"rayclusters.ray.io",
		"rayjobs.ray.io",
		"rayservices.ray.io",
	}
	for _, name := range wantCRDs {
		crd := &apiextv1.CustomResourceDefinition{}
		if err := c.Get(ctx, client.ObjectKey{Name: name}, crd); err != nil {
			t.Errorf("KubeRay CRD %q not registered on the cluster: %v", name, err)
			continue
		}
		if !crdEstablished(crd) {
			t.Errorf("KubeRay CRD %q registered but not Established/served", name)
		}
	}
}

// crdEstablished reports whether the apiserver has Established the CRD (its
// resources are served).
func crdEstablished(crd *apiextv1.CustomResourceDefinition) bool {
	for _, cond := range crd.Status.Conditions {
		if cond.Type == apiextv1.Established && cond.Status == apiextv1.ConditionTrue {
			return true
		}
	}
	return false
}

// TestKubeRayOperatorAvailable asserts the KubeRay operator deployment exists and
// is available (installed by `make e2e-up` into the kuberay-system namespace).
func TestKubeRayOperatorAvailable(t *testing.T) {
	c := e2eClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dep := &appsv1.Deployment{}
	key := client.ObjectKey{Namespace: "kuberay-system", Name: "kuberay-operator"}
	if err := c.Get(ctx, key, dep); err != nil {
		if apierrors.IsNotFound(err) {
			t.Fatalf("kuberay-operator deployment not found in kuberay-system (run `make e2e-up`)")
		}
		t.Fatalf("get kuberay-operator deployment: %v", err)
	}

	if !deploymentAvailable(dep) {
		t.Errorf("kuberay-operator deployment exists but is not Available (readyReplicas=%d)", dep.Status.ReadyReplicas)
	}
}

// deploymentAvailable reports whether the deployment's Available condition is
// True.
func deploymentAvailable(dep *appsv1.Deployment) bool {
	for _, cond := range dep.Status.Conditions {
		if cond.Type == appsv1.DeploymentAvailable && cond.Status == "True" {
			return true
		}
	}
	return false
}
