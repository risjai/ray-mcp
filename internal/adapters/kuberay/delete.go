package kuberay

import (
	"context"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// Delete removes a KubeRay CRD resource by kind/namespace/name. Deleting the
// RayCluster cascades to its owned pods/services via ownerReferences (KubeRay
// sets controller refs on the head/worker pods and head service); we delete the
// CR only and let background GC reap the children. No PropagationPolicy is set
// (the default Background cascading delete applies).
//
// When dryRun is true the API server validates existence and RBAC but persists
// nothing — the resource is NOT deleted; this is the server-side validation-only
// path for the destructive tier's confirm-then-commit flow.
func (c *Client) Delete(ctx context.Context, kind domain.Kind, namespace, name string, dryRun bool) error {
	k8s, err := c.ensureClient()
	if err != nil {
		return err
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(rayv1.GroupVersion.WithKind(string(kind)))
	obj.SetNamespace(namespace)
	obj.SetName(name)

	opts := []client.DeleteOption{}
	verb := "delete"
	if dryRun {
		opts = append(opts, client.DryRunAll)
		verb = "dry-run delete"
	}

	if err := k8s.Delete(ctx, obj, opts...); err != nil {
		return mapK8sError(err, verb, kind, namespace, name)
	}
	return nil
}
