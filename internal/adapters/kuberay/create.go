package kuberay

import (
	"fmt"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/risjai/ray-mcp/internal/domain"
)

// rayHeadContainerName / rayWorkerContainerName are the conventional container
// names KubeRay uses in its own samples. The operator injects the Ray start
// command into the FIRST container (RayContainerIndex = 0), NOT by matching the
// name, so any name works; the curated base uses these for readability of the
// resulting pod spec.
const (
	rayHeadContainerName   = "ray-head"
	rayWorkerContainerName = "ray-worker"
	// gpuResourceName is the extended resource the curated `gpu` quantity maps to.
	// nvidia.com/gpu is the de-facto standard advertised by the NVIDIA device
	// plugin; non-NVIDIA accelerators use the rawSpec escape hatch (Gate 1 C3).
	gpuResourceName = "nvidia.com/gpu"
)

// BuildClusterBase implements domain.ClusterBaseBuilder: it turns the curated half
// of the create params into the base unstructured RayCluster (spec §7.C step 1:
// curated params → typed KubeRay object → JSON map). It builds a typed
// rayv1.RayCluster so the curated shape is validated by Go's type system and the
// KubeRay field names/JSON tags are authoritative, then converts to a plain map
// via the unstructured converter. The result becomes the BASE for Merge; it does
// NOT apply RawSpec (that is the domain's Merge step, kept separate so rawSpec-wins
// + identity-guard semantics live in one place).
//
// The typed round-trip here is the curated base ONLY — it is sound because the
// curated params are by construction expressible in the compiled KubeRay types.
// The wedge (fields newer than the pin) rides in via RawSpec, which Merge applies
// over this base as raw JSON and never round-trips through a typed struct.
func (c *Client) BuildClusterBase(p domain.ClusterCreateParams) (domain.MergedSpec, error) {
	rc := &rayv1.RayCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rayv1.GroupVersion.String(),
			Kind:       "RayCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.Name,
			Namespace:   p.Namespace,
			Labels:      p.Labels,
			Annotations: p.Annotations,
		},
		Spec: rayv1.RayClusterSpec{
			RayVersion: p.RayVersion,
		},
	}

	if p.EnableAutoscaling {
		enabled := true
		rc.Spec.EnableInTreeAutoscaling = &enabled
	}

	headContainer, err := rayContainer(rayHeadContainerName, p.Image, p.HeadResources)
	if err != nil {
		return nil, fmt.Errorf("head group: %w", err)
	}
	rc.Spec.HeadGroupSpec = rayv1.HeadGroupSpec{
		RayStartParams: map[string]string{},
		Template:       podTemplate(headContainer),
	}

	for _, wg := range p.WorkerGroups {
		workerContainer, err := rayContainer(rayWorkerContainerName, p.Image, wg.Resources)
		if err != nil {
			return nil, fmt.Errorf("worker group %q: %w", wg.Name, err)
		}
		rc.Spec.WorkerGroupSpecs = append(rc.Spec.WorkerGroupSpecs, workerGroupSpec(wg, workerContainer))
	}

	base, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rc)
	if err != nil {
		return nil, fmt.Errorf("convert curated RayCluster %q to unstructured: %w", p.Name, err)
	}
	return base, nil
}

// workerGroupSpec builds one typed worker group from the curated params. It
// keeps maxReplicas a valid ceiling: KubeRay validation rejects a worker group
// whose minReplicas > maxReplicas, and a max below the desired replicas would
// silently under-cap the group. A caller that leaves MaxReplicas at the Go zero
// value would hit both, so MaxReplicas is clamped UP to max(replicas, minReplicas)
// when it was left below that floor. (A caller wanting autoscaler headroom should
// set maxReplicas explicitly; the clamp only ever raises it, never lowers it.)
func workerGroupSpec(wg domain.WorkerGroupParams, container corev1.Container) rayv1.WorkerGroupSpec {
	replicas := wg.Replicas
	minReplicas := wg.MinReplicas
	maxReplicas := wg.MaxReplicas
	if floor := max(replicas, minReplicas); maxReplicas < floor {
		maxReplicas = floor
	}
	return rayv1.WorkerGroupSpec{
		GroupName:      wg.Name,
		Replicas:       &replicas,
		MinReplicas:    &minReplicas,
		MaxReplicas:    &maxReplicas,
		RayStartParams: map[string]string{},
		Template:       podTemplate(container),
	}
}

// podTemplate wraps a single Ray container in the minimal pod template KubeRay
// needs. The richer pod surface (volumes, multiple containers, nodeSelector,
// tolerations) is the rawSpec escape hatch's domain, not the curated shape.
func podTemplate(container corev1.Container) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{container},
		},
	}
}

// rayContainer builds the Ray container with the image and the curated resource
// quantities mapped to requests+limits (cpu/memory/gpu). A resource is omitted
// when its quantity string is empty; gpu maps to the nvidia.com/gpu extended
// resource on both requests and limits (the device-plugin convention). An
// unparseable quantity is a hard error so a malformed create fails before any
// cluster call rather than producing a silently-wrong spec.
func rayContainer(name, image string, r domain.ResourceQuantities) (corev1.Container, error) {
	container := corev1.Container{Name: name, Image: image}

	requests := corev1.ResourceList{}
	limits := corev1.ResourceList{}

	if err := addQuantity(requests, limits, corev1.ResourceCPU, r.CPU); err != nil {
		return corev1.Container{}, fmt.Errorf("cpu: %w", err)
	}
	if err := addQuantity(requests, limits, corev1.ResourceMemory, r.Memory); err != nil {
		return corev1.Container{}, fmt.Errorf("memory: %w", err)
	}
	if err := addQuantity(requests, limits, gpuResourceName, r.GPU); err != nil {
		return corev1.Container{}, fmt.Errorf("gpu: %w", err)
	}

	if len(requests) > 0 {
		container.Resources.Requests = requests
	}
	if len(limits) > 0 {
		container.Resources.Limits = limits
	}
	return container, nil
}

// addQuantity parses a curated quantity string and, when non-empty, sets it on
// both the requests and limits lists under name. CPU/memory/gpu are all set on
// requests and limits identically — a deliberately simple curated default;
// asymmetric requests-vs-limits is a rawSpec concern.
func addQuantity(requests, limits corev1.ResourceList, name corev1.ResourceName, value string) error {
	if value == "" {
		return nil
	}
	q, err := resource.ParseQuantity(value)
	if err != nil {
		return fmt.Errorf("invalid quantity %q: %w", value, err)
	}
	requests[name] = q
	limits[name] = q
	return nil
}
