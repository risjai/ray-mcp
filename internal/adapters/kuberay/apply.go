package kuberay

import (
	"context"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// fieldManager is the Server-Side Apply field manager ray-mcp owns. Every
// mutation is attributed to it in the object's managedFields, which is how SSA
// keeps ray-mcp's writes from clobbering fields another manager owns — notably
// the Ray autoscaler's ownership of worker `replicas` (spec §7.D). By default we
// do NOT pass client.ForceOwnership, so a genuine co-ownership conflict surfaces
// as a domain.ConflictError; update/scale opt into force only on a conflict-retry
// (opts.Force, Task 10), having re-read the live object so the forced apply
// re-asserts the contended value rather than clobbering it.
const fieldManager = "ray-mcp"

// Apply is the unified Server-Side Apply write path for create/update/scale/
// deploy (spec §7.C steps 6-7). It wraps the merged unstructured spec as an
// apply configuration via ApplyConfigurationFromUnstructured (which marshals the
// raw map — NO client-side round-trip through the compiled KubeRay Go types), so
// fields newer than our module pin survive to the API server. That is the wedge
// guarantee (Q5 step 5): validation happens server-side against the INSTALLED
// CRD schema, not client-side against the compiled baseline. When dryRun is true
// it adds client.DryRunAll: the API server fully validates and (for a structural
// CRD) rejects unknown fields with a hard error, but persists nothing — the
// pipeline's validation oracle (Q4/B2). It returns the server's read-back as a
// plain map.
//
// The incoming spec is deep-copied before use. The SetGroupVersionKind/
// SetNamespace/SetName calls below mutate obj.Object IN PLACE; if obj.Object
// aliased the caller's spec map, those writes — plus the read-back client.Apply
// decodes into obj — would corrupt the caller's intent. The domain calls Apply
// twice with the SAME intent map (dry-run then commit), so isolating each call
// from the caller's map is load-bearing, not merely hygienic. spec is expected
// to be JSON-compatible (it originates from the Task 8a JSON merge); that is the
// precondition of the MergedSpec contract. apiVersion/kind and name/namespace are
// set authoritatively from the typed args so the apply always targets exactly
// kind/namespace/name (SSA requires all four) — Merge already identity-guards
// the map, so this is a backstop, not a divergence.
func (c *Client) Apply(ctx context.Context, kind domain.Kind, namespace, name string, spec domain.MergedSpec, applyOpts domain.ApplyOptions) (domain.MergedSpec, error) {
	k8s, err := c.ensureClient()
	if err != nil {
		return nil, err
	}

	obj := &unstructured.Unstructured{Object: runtime.DeepCopyJSON(spec)}
	obj.SetGroupVersionKind(rayv1.GroupVersion.WithKind(string(kind)))
	obj.SetNamespace(namespace)
	obj.SetName(name)

	opts := []client.ApplyOption{client.FieldOwner(fieldManager)}
	verb := "apply"
	if applyOpts.DryRun {
		opts = append(opts, client.DryRunAll)
		verb = "dry-run apply"
	}
	if applyOpts.Force {
		// ForceOwnership: take over fields another manager owns. update/scale set
		// this only on a conflict-retry, after re-reading the live object so the
		// contended value (e.g. autoscaler replicas) is re-asserted, not clobbered.
		opts = append(opts, client.ForceOwnership)
	}

	if err := k8s.Apply(ctx, client.ApplyConfigurationFromUnstructured(obj), opts...); err != nil {
		return nil, mapK8sError(err, verb, kind, namespace, name)
	}

	// obj.Object now holds the server's view (client.Apply read the response back
	// into obj). Return it as the plain-map MergedSpec the domain diffs.
	return domain.MergedSpec(obj.Object), nil
}
