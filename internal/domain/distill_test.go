package domain

import "testing"

// TestHealthLine pins the kind-agnostic health-line join: segments are joined
// with "; ", and empty segments are dropped so a missing detail never leaves a
// dangling separator. The cluster rows are the concrete (input → distilled)
// examples from the status-distillation design note §5; the degrade row shows
// the same join serving a non-cluster kind (the point of the shared composer,
// §6). Table-driven per Task 13's "table-driven tests over the fixtures".
func TestHealthLine(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		segments []string
		want     string
	}{
		{
			name:     "RayCluster healthy (note §5)",
			segments: []string{"Ready", "2/2 workers ready"},
			want:     "Ready; 2/2 workers ready",
		},
		{
			name:     "RayCluster wedged on GPU (note §5)",
			segments: []string{"Provisioning", "0/2 workers ready", "unschedulable: insufficient nvidia.com/gpu"},
			want:     "Provisioning; 0/2 workers ready; unschedulable: insufficient nvidia.com/gpu",
		},
		{
			name:     "empty detail is dropped (no dangling separator)",
			segments: []string{"Ready", "2/2 workers ready", ""},
			want:     "Ready; 2/2 workers ready",
		},
		{
			name:     "empty middle segment is dropped",
			segments: []string{"Ready", "", "scaling to 4"},
			want:     "Ready; scaling to 4",
		},
		{
			name:     "empty leading segment is dropped (position-independent)",
			segments: []string{"", "2/2 workers ready", "scaling to 4"},
			want:     "2/2 workers ready; scaling to 4",
		},
		{
			name:     "RayJob degrade line composes via the same join (note §5)",
			segments: []string{"Initializing", "live Ray detail unavailable: connection refused"},
			want:     "Initializing; live Ray detail unavailable: connection refused",
		},
		{
			name:     "single segment, no separator",
			segments: []string{"Unknown"},
			want:     "Unknown",
		},
		{
			name:     "no segments yields empty",
			segments: nil,
			want:     "",
		},
		{
			name:     "all segments empty yields empty",
			segments: []string{"", ""},
			want:     "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := HealthLine(tc.segments...); got != tc.want {
				t.Errorf("HealthLine(%q) = %q, want %q", tc.segments, got, tc.want)
			}
		})
	}
}

// TestConditionReason pins the compact reason/message rendering shared across
// kinds: "Reason: Message" when both are present, else whichever one is set,
// else empty. The adapter reads the k8s-typed condition and passes the extracted
// strings in, keeping this pure (design note §6).
func TestConditionReason(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		reason  string
		message string
		want    string
	}{
		{
			name:    "both present joins with colon",
			reason:  "FailedScheduling",
			message: "0/3 nodes insufficient nvidia.com/gpu",
			want:    "FailedScheduling: 0/3 nodes insufficient nvidia.com/gpu",
		},
		{
			name:    "message only",
			reason:  "",
			message: "pods are being provisioned",
			want:    "pods are being provisioned",
		},
		{
			name:    "reason only",
			reason:  "RayClusterProvisioning",
			message: "",
			want:    "RayClusterProvisioning",
		},
		{
			name:    "both empty yields empty",
			reason:  "",
			message: "",
			want:    "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ConditionReason(tc.reason, tc.message); got != tc.want {
				t.Errorf("ConditionReason(%q, %q) = %q, want %q", tc.reason, tc.message, got, tc.want)
			}
		})
	}
}
