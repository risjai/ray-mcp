package kuberay

import (
	"fmt"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/risjai/ray-mcp/internal/domain"
)

// rayJobClusterSelectorKey is the key KubeRay reads from spec.clusterSelector to
// resolve the target RayCluster's NAME in existing-cluster mode (it does not run a
// label-selector query in this path — it pulls the single value at this key and
// treats it as the cluster name). It mirrors KubeRay's RayJobClusterSelectorKey
// constant (v1.6.1); the domain must not import the KubeRay packages, so the
// constant lives here at the adapter edge.
const rayJobClusterSelectorKey = "ray.io/cluster"

// BuildJobBase implements domain.JobBaseBuilder: it turns the curated half of the
// submit params into the base unstructured RayJob (spec §7.C step 1: curated
// params → typed KubeRay object → JSON map). It builds a typed rayv1.RayJob so the
// curated shape is validated by Go's type system and the KubeRay field names/JSON
// tags are authoritative, then converts to a plain map. The result becomes the
// BASE for Merge; it does NOT apply RawSpec (that is the domain's Merge step).
//
// The service has already validated the mode XOR and resolved the ephemeral
// shutdown default before calling this, so exactly one of ExistingCluster /
// ClusterSpec is set, and ShutdownAfterJobFinishes (when relevant) is a concrete
// decision. Status is never set here — it is a server-owned subresource the
// controller populates after the apply.
func (c *Client) BuildJobBase(p domain.JobSubmitParams) (domain.MergedSpec, error) {
	rj := &rayv1.RayJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rayv1.GroupVersion.String(),
			Kind:       "RayJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint:     p.Entrypoint,
			RuntimeEnvYAML: p.RuntimeEnvYAML,
			Metadata:       p.Metadata,
		},
	}

	switch {
	case p.ExistingCluster != "":
		// Existing-cluster mode: target a running RayCluster by name. No cluster is
		// created or deleted; shutdownAfterJobFinishes is never set (the service
		// rejects it in this mode), so KubeRay never tears down a cluster we don't own.
		rj.Spec.ClusterSelector = map[string]string{rayJobClusterSelectorKey: p.ExistingCluster}
	case p.ClusterSpec != nil:
		// Ephemeral mode: KubeRay creates a cluster from this template for the job.
		// shutdownAfterJobFinishes is set on the unstructured base AFTER conversion
		// (below), not here — the typed field is `omitempty`, so a concrete false
		// would be dropped, and the "pass false to keep the cluster for debugging"
		// contract (Q16b) requires ray-mcp to OWN the field via SSA rather than
		// leave a false to KubeRay's matching-but-incidental default.
		spec, err := rayClusterSpec(*p.ClusterSpec)
		if err != nil {
			return nil, err
		}
		rj.Spec.RayClusterSpec = spec
	default:
		// Unreachable: the service enforces exactly-one-mode before calling. Guard
		// anyway so a future caller that skips validation fails loudly, not silently.
		return nil, fmt.Errorf("internal: BuildJobBase called with no cluster target")
	}

	base, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rj)
	if err != nil {
		return nil, fmt.Errorf("convert curated RayJob %q to unstructured: %w", p.Name, err)
	}

	// Set shutdownAfterJobFinishes directly on the map so an explicit false is
	// carried (the typed field's `omitempty` drops it on conversion). The service
	// only leaves this non-nil in ephemeral mode, so ray-mcp asserts ownership of
	// the field there and never in existing-cluster mode.
	if p.ShutdownAfterJobFinishes != nil {
		spec, _ := base["spec"].(map[string]any)
		if spec == nil {
			return nil, fmt.Errorf("internal: converted RayJob %q has no spec map", p.Name)
		}
		spec["shutdownAfterJobFinishes"] = *p.ShutdownAfterJobFinishes
	}
	return base, nil
}

// rayClusterSpec builds the typed embedded RayClusterSpec for the ephemeral mode
// from the curated cluster knobs, reusing the same head/worker construction as the
// standalone RayCluster create (so an ephemeral cluster and a standalone cluster
// share one curated shape). Identity is deliberately NOT set: the RayJob owns the
// job identity and KubeRay generates the cluster name.
func rayClusterSpec(cs domain.ClusterSubmitSpec) (*rayv1.RayClusterSpec, error) {
	spec := &rayv1.RayClusterSpec{RayVersion: cs.RayVersion}

	if cs.EnableAutoscaling {
		enabled := true
		spec.EnableInTreeAutoscaling = &enabled
	}

	headContainer, err := rayContainer(rayHeadContainerName, cs.Image, cs.HeadResources)
	if err != nil {
		return nil, fmt.Errorf("head group: %w", err)
	}
	spec.HeadGroupSpec = rayv1.HeadGroupSpec{
		RayStartParams: map[string]string{},
		Template:       podTemplate(headContainer),
	}

	for _, wg := range cs.WorkerGroups {
		workerContainer, err := rayContainer(rayWorkerContainerName, cs.Image, wg.Resources)
		if err != nil {
			return nil, fmt.Errorf("worker group %q: %w", wg.Name, err)
		}
		spec.WorkerGroupSpecs = append(spec.WorkerGroupSpecs, workerGroupSpec(wg, workerContainer))
	}

	return spec, nil
}
