package kuberay

import (
	"fmt"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/risjai/ray-mcp/internal/domain"
)

// BuildServiceBase implements domain.ServiceBaseBuilder: it turns the curated half
// of ServiceDeployParams into the base unstructured RayService (typed KubeRay
// object → JSON map). The embedded cluster spec uses the JSON key "rayClusterConfig"
// (Go field RayClusterSpec — the documented trap). serveConfigV2 is optional.
// The result becomes the BASE for Merge; it does NOT apply RawSpec.
func (c *Client) BuildServiceBase(p domain.ServiceDeployParams) (domain.MergedSpec, error) {
	rs := &rayv1.RayService{
		TypeMeta: metav1.TypeMeta{
			APIVersion: rayv1.GroupVersion.String(),
			Kind:       "RayService",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:        p.Name,
			Namespace:   p.Namespace,
			Labels:      p.Labels,
			Annotations: p.Annotations,
		},
		Spec: rayv1.RayServiceSpec{
			ServeConfigV2: p.ServeConfigV2,
		},
	}

	// Build the embedded RayClusterSpec (the same curated shape as standalone
	// cluster create, minus identity). Reuses rayClusterSpec from job_submit.go.
	rcs, err := rayClusterSpec(domain.ClusterSubmitSpec{
		RayVersion:        p.RayVersion,
		Image:             p.Image,
		HeadResources:     p.HeadResources,
		WorkerGroups:      p.WorkerGroups,
		EnableAutoscaling: p.EnableAutoscaling,
	})
	if err != nil {
		return nil, err
	}
	rs.Spec.RayClusterSpec = *rcs

	base, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rs)
	if err != nil {
		return nil, fmt.Errorf("convert curated RayService %q to unstructured: %w", p.Name, err)
	}
	return base, nil
}
