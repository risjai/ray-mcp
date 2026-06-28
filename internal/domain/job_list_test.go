package domain

import (
	"context"
	"testing"
)

// newJobListFake seeds a fakeKubeRay with the given job details for the list
// path (keyed by namespace/name), mirroring newJobFake.
func newJobListFake(details ...JobDetail) *fakeKubeRay {
	jobs := make(map[string]JobDetail, len(details))
	for _, d := range details {
		jobs[key(d.Namespace, d.Name)] = d
	}
	return &fakeKubeRay{jobs: jobs}
}

// listJob builds a JobDetail whose embedded summary carries both the Ray-side
// phase (JobStatus) and the CRD lifecycle (JobDeploymentStatus) — the two status
// fields ray_job_list must surface side by side.
func listJob(namespace, name, jobStatus, deploymentStatus string) JobDetail {
	return JobDetail{
		JobSummary: JobSummary{
			Name:                name,
			Namespace:           namespace,
			JobStatus:           jobStatus,
			JobDeploymentStatus: deploymentStatus,
		},
	}
}

// TestJobListDefaultsNamespace asserts an omitted request namespace falls back to
// the service default and that default is what reaches the port.
func TestJobListDefaultsNamespace(t *testing.T) {
	t.Parallel()

	fake := newJobListFake(listJob("ray-system", "trainer", "RUNNING", "Running"))
	svc := NewJobService(fake, nil, nil, "ray-system")

	res, err := svc.List(context.Background(), JobListRequest{Namespace: ""})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(res.Items) != 1 || res.Items[0].Name != "trainer" {
		t.Fatalf("List items = %+v, want exactly the trainer job", res.Items)
	}
	if fake.lastListOpts.AllNamespaces {
		t.Errorf("AllNamespaces leaked true into the port for a defaulted-namespace list")
	}
}

// TestJobListRowCarriesBothStatuses is the spec's headline requirement: each row
// surfaces BOTH the CRD deployment status (status.jobDeploymentStatus) AND the
// Ray-side job status (status.jobStatus), not one or the other.
func TestJobListRowCarriesBothStatuses(t *testing.T) {
	t.Parallel()

	fake := newJobListFake(listJob("default", "demo", "SUCCEEDED", "Complete"))
	svc := NewJobService(fake, nil, nil, "default")

	res, err := svc.List(context.Background(), JobListRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(res.Items) != 1 {
		t.Fatalf("List returned %d items, want 1", len(res.Items))
	}
	row := res.Items[0]
	if row.JobStatus != "SUCCEEDED" {
		t.Errorf("row.JobStatus = %q, want SUCCEEDED (the Ray-side phase)", row.JobStatus)
	}
	if row.JobDeploymentStatus != "Complete" {
		t.Errorf("row.JobDeploymentStatus = %q, want Complete (the CRD lifecycle)", row.JobDeploymentStatus)
	}
}

// TestJobListAllNamespacesPassThrough asserts AllNamespaces reaches the port and
// returns jobs across namespaces.
func TestJobListAllNamespacesPassThrough(t *testing.T) {
	t.Parallel()

	fake := newJobListFake(
		listJob("team-a", "a", "RUNNING", "Running"),
		listJob("team-b", "b", "PENDING", "Initializing"),
	)
	svc := NewJobService(fake, nil, nil, "default")

	res, err := svc.List(context.Background(), JobListRequest{AllNamespaces: true})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if !fake.lastListOpts.AllNamespaces {
		t.Errorf("AllNamespaces did not pass through to the port; lastListOpts = %+v", fake.lastListOpts)
	}
	if len(res.Items) != 2 {
		t.Fatalf("List returned %d items, want 2 across namespaces", len(res.Items))
	}
}

// TestJobListMoreAvailableFromContinueToken asserts the "more available vs showing
// all" signal is derived purely from the continue token, never a fabricated total.
func TestJobListMoreAvailableFromContinueToken(t *testing.T) {
	t.Parallel()

	t.Run("token present -> more available", func(t *testing.T) {
		t.Parallel()
		fake := newJobListFake(listJob("default", "a", "RUNNING", "Running"))
		fake.listJobsContinue = "next-page-token"
		svc := NewJobService(fake, nil, nil, "default")

		res, err := svc.List(context.Background(), JobListRequest{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if !res.MoreAvailable {
			t.Errorf("MoreAvailable = false, want true (continue token present)")
		}
		if res.Continue != "next-page-token" {
			t.Errorf("Continue = %q, want it surfaced verbatim", res.Continue)
		}
	})

	t.Run("no token -> showing all", func(t *testing.T) {
		t.Parallel()
		fake := newJobListFake(listJob("default", "a", "RUNNING", "Running"))
		svc := NewJobService(fake, nil, nil, "default")

		res, err := svc.List(context.Background(), JobListRequest{})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if res.MoreAvailable {
			t.Errorf("MoreAvailable = true, want false (no continue token)")
		}
		if res.Continue != "" {
			t.Errorf("Continue = %q, want empty", res.Continue)
		}
	})
}

// TestJobListPassesLimitThrough asserts the limit + continue are passed to the
// port unchanged (the adapter, not the domain, applies the 0→50 default).
func TestJobListPassesLimitThrough(t *testing.T) {
	t.Parallel()

	fake := newJobListFake()
	svc := NewJobService(fake, nil, nil, "default")

	if _, err := svc.List(context.Background(), JobListRequest{Limit: 7, Continue: "tok"}); err != nil {
		t.Fatalf("List: %v", err)
	}
	if fake.lastListOpts.Limit != 7 {
		t.Errorf("Limit = %d, want 7 passed through", fake.lastListOpts.Limit)
	}
	if fake.lastListOpts.Continue != "tok" {
		t.Errorf("Continue = %q, want %q passed through", fake.lastListOpts.Continue, "tok")
	}
}
