package domain

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeClock is a virtual clock for the bounded-wait loop: sleep advances now by
// the slept duration, so the wait loop terminates deterministically without real
// time. Tests inject it by setting the service's now/sleep seams directly (same
// package), keeping NewJobService's signature unchanged.
type fakeClock struct {
	t      time.Time
	sleeps int // number of sleep calls, to assert poll cadence.
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.Unix(1_700_000_000, 0)} }

func (c *fakeClock) now() time.Time { return c.t }

func (c *fakeClock) sleep(_ context.Context, d time.Duration) error {
	c.sleeps++
	c.t = c.t.Add(d)
	return nil
}

// withClock wires a fake clock into the service's wait seams.
func withClock(svc *JobService, c *fakeClock) *JobService {
	svc.now = c.now
	svc.sleep = c.sleep
	return svc
}

// TestJobWaitRunningReachedWhenDashboardRunning is the happy path: a scheduled
// job whose dashboard reports RUNNING satisfies until=running on the first poll
// and returns reached=true without sleeping.
func TestJobWaitRunningReachedWhenDashboardRunning(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	kube := newJobFake(job)
	api := &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: "RUNNING"},
	}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	clock := newFakeClock()
	svc := withClock(NewJobService(kube, reach, api, "default"), clock)

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 30})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Reached {
		t.Errorf("Reached = false, want true for a RUNNING job under until=running")
	}
	if res.Live == nil || res.Live.Status != "RUNNING" {
		t.Errorf("Live = %+v, want the dialed RUNNING status", res.Live)
	}
	if clock.sleeps != 0 {
		t.Errorf("sleeps = %d, want 0 (reached on the first poll, no waiting)", clock.sleeps)
	}
}

// pendingJob is a scheduled job whose dashboard reports PENDING — past the dial
// gate but the Ray driver has not started executing.
func pendingDashboard() *fakeRayAPI {
	return &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: "PENDING"},
	}}
}

// TestJobWaitRunningStuckPendingReturnsNotReachedAtCap asserts a job that stays
// PENDING for the whole bound returns reached=false at the cap (the honest
// "stuck Pending" answer), having polled across the window — never an error.
func TestJobWaitRunningStuckPendingReturnsNotReachedAtCap(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	clock := newFakeClock()
	svc := withClock(NewJobService(kube, reach, pendingDashboard(), "default"), clock)

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 10})
	if err != nil {
		t.Fatalf("Wait: %v (a stuck-Pending job must not error)", err)
	}
	if res.Reached {
		t.Errorf("Reached = true, want false for a job stuck PENDING")
	}
	if res.Live == nil || res.Live.Status != "PENDING" {
		t.Errorf("Live = %+v, want the last-observed PENDING status", res.Live)
	}
	if clock.sleeps == 0 {
		t.Errorf("sleeps = 0, want > 0 (must poll across the wait window before giving up)")
	}
}

// TestJobWaitRunningNotScheduledNoDial asserts that before the dial gate
// (not scheduled, not CRD-terminal) until=running returns reached=false WITHOUT
// dialing — answered by phase 1 alone. A nil api/reach would panic on a dial.
func TestJobWaitRunningNotScheduledNoDial(t *testing.T) {
	t.Parallel()

	job := JobDetail{
		JobSummary:          JobSummary{Name: "early", Namespace: "default"},
		JobDeploymentStatus: "Initializing",
	}
	kube := newJobFake(job)
	clock := newFakeClock()
	// nil reach + nil api: any dial attempt panics, proving phase 1 answered.
	svc := withClock(NewJobService(kube, nil, nil, "default"), clock)

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "early", Until: "running", WaitSeconds: 0})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Reached {
		t.Errorf("Reached = true, want false for a not-yet-scheduled job")
	}
	if res.Scheduled {
		t.Errorf("Scheduled = true, want false")
	}
	if res.Detail.JobDeploymentStatus != "Initializing" {
		t.Errorf("Detail.JobDeploymentStatus = %q, want Initializing (CRD view returned)", res.Detail.JobDeploymentStatus)
	}
}

// TestJobWaitCRDTerminalReachedWithoutDial asserts the critical pre-scheduling
// failure case: a RayJob that terminally fails on the CRD (jobDeploymentStatus
// Failed) BEFORE status.jobId is ever set is observed as reached for BOTH
// conditions, without dialing — the only signal, since the dashboard could never
// answer. Verified vs KubeRay v1.6.1 (IsJobDeploymentTerminal + ValidationFailed).
func TestJobWaitCRDTerminalReachedWithoutDial(t *testing.T) {
	t.Parallel()

	for _, until := range []string{"running", "terminal"} {
		for _, status := range []string{"Failed", "Complete", "ValidationFailed"} {
			job := JobDetail{
				JobSummary:          JobSummary{Name: "dead", Namespace: "default"},
				JobDeploymentStatus: status, // terminal on the CRD; no jobId/dashboardURL ever set.
			}
			kube := newJobFake(job)
			// nil reach + nil api: a dial would panic, proving CRD-terminal short-circuits.
			svc := withClock(NewJobService(kube, nil, nil, "default"), newFakeClock())

			res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "dead", Until: until, WaitSeconds: 30})
			if err != nil {
				t.Fatalf("until=%s status=%s: Wait: %v", until, status, err)
			}
			if !res.Reached {
				t.Errorf("until=%s status=%s: Reached = false, want true (CRD-terminal)", until, status)
			}
		}
	}
}

// TestJobWaitRunningReachedWhenAlreadyTerminal asserts the "a finished job
// obviously started" semantics: a job whose dashboard already reports a terminal
// status satisfies until=running (only PENDING means not-yet-running). Verified
// vs KubeRay v1.6.1 (Ray JobStatus enum).
func TestJobWaitRunningReachedWhenAlreadyTerminal(t *testing.T) {
	t.Parallel()

	for _, status := range []string{"SUCCEEDED", "FAILED", "STOPPED"} {
		kube := newJobFake(scheduledJob("default", "demo"))
		api := &fakeRayAPI{status: map[string]RayJobStatus{
			"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: status},
		}}
		reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
		svc := withClock(NewJobService(kube, reach, api, "default"), newFakeClock())

		res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 30})
		if err != nil {
			t.Fatalf("status=%s: Wait: %v", status, err)
		}
		if !res.Reached {
			t.Errorf("status=%s: Reached = false, want true (a finished job obviously started)", status)
		}
	}
}

// TestJobWaitTerminalReachedWhenSucceeded asserts until=terminal is reached on a
// SUCCEEDED dashboard status.
func TestJobWaitTerminalReachedWhenSucceeded(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	api := &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: "SUCCEEDED"},
	}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := withClock(NewJobService(kube, reach, api, "default"), newFakeClock())

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "terminal", WaitSeconds: 30})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if !res.Reached {
		t.Errorf("Reached = false, want true for a SUCCEEDED job under until=terminal")
	}
}

// TestJobWaitTerminalNotReachedWhenRunning asserts until=terminal keeps polling
// (reached=false at cap) for a RUNNING job — running is not terminal.
func TestJobWaitTerminalNotReachedWhenRunning(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	api := &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: "RUNNING"},
	}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	clock := newFakeClock()
	svc := withClock(NewJobService(kube, reach, api, "default"), clock)

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "terminal", WaitSeconds: 6})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Reached {
		t.Errorf("Reached = true, want false for a RUNNING job under until=terminal")
	}
	if clock.sleeps == 0 {
		t.Errorf("sleeps = 0, want > 0 (RUNNING is not terminal; must poll to the cap)")
	}
}

// TestJobWaitDefaultsToRunning asserts an empty Until defaults to "running".
func TestJobWaitDefaultsToRunning(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	api := &fakeRayAPI{status: map[string]RayJobStatus{
		"raysubmit_abc123": {JobID: "raysubmit_abc123", Status: "RUNNING"},
	}}
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := withClock(NewJobService(kube, reach, api, "default"), newFakeClock())

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", WaitSeconds: 30}) // Until omitted.
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Until != "running" {
		t.Errorf("Until = %q, want running (default)", res.Until)
	}
	if !res.Reached {
		t.Errorf("Reached = false, want true (RUNNING satisfies the default until=running)")
	}
}

// TestJobWaitClampsWaitSeconds asserts a request above the cap polls no longer
// than maxWaitSeconds (30s) of virtual time, proving the bound is enforced.
func TestJobWaitClampsWaitSeconds(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	clock := newFakeClock()
	start := clock.now()
	svc := withClock(NewJobService(kube, reach, pendingDashboard(), "default"), clock)

	if _, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 9000}); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if elapsed := clock.now().Sub(start); elapsed > maxWaitSeconds*time.Second {
		t.Errorf("virtual elapsed = %s, want <= %ds (WaitSeconds must be clamped to the cap)", elapsed, maxWaitSeconds)
	}
}

// TestJobWaitRunningNotReachedWhenCRDRunningButDashboardPending is the gotcha:
// jobDeploymentStatus=="Running" is an infra-lifecycle phase, NOT proof the Ray
// job is executing. With a scheduled job whose dashboard still reports PENDING,
// until=running must NOT report reached — the dashboard is authoritative.
func TestJobWaitRunningNotReachedWhenCRDRunningButDashboardPending(t *testing.T) {
	t.Parallel()

	job := scheduledJob("default", "demo")
	job.JobDeploymentStatus = "Running" // CRD says Running, but...
	kube := newJobFake(job)
	clock := newFakeClock()
	svc := withClock(NewJobService(kube, &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}, pendingDashboard(), "default"), clock)

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 4})
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if res.Reached {
		t.Errorf("Reached = true, want false: jobDeploymentStatus=Running must not be read as the driver running while the dashboard is PENDING")
	}
}

// TestJobWaitDegradesAtCapWhenDashboardUnreachable asserts a scheduled job whose
// dashboard is unreachable degrades each poll (reached=false, no hard error) and,
// at the cap, returns the degraded CRD-derived view with the bounded reason.
func TestJobWaitDegradesAtCapWhenDashboardUnreachable(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	reach := erroringReachability{err: &RayAPIUnreachableError{Endpoint: "default/demo-cluster", Reason: "connection refused"}}
	clock := newFakeClock()
	svc := withClock(NewJobService(kube, reach, nil, "default"), clock) // nil api: must never be dialed.

	res, err := svc.Wait(context.Background(), JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 8})
	if err != nil {
		t.Fatalf("Wait degraded path returned a hard error: %v", err)
	}
	if res.Reached {
		t.Errorf("Reached = true, want false when the dashboard never became reachable")
	}
	if !res.Degraded || res.DegradeReason != "connection refused" {
		t.Errorf("got Degraded=%v DegradeReason=%q, want true/\"connection refused\"", res.Degraded, res.DegradeReason)
	}
	if res.Live != nil {
		t.Errorf("Live = %+v, want nil when degraded", res.Live)
	}
}

// TestJobWaitNotFoundPropagates asserts a missing job surfaces the typed
// NotFoundError unchanged (a missing CRD is a real error, not a "not reached").
func TestJobWaitNotFoundPropagates(t *testing.T) {
	t.Parallel()

	svc := withClock(NewJobService(newJobFake(), nil, nil, "default"), newFakeClock())

	_, err := svc.Wait(context.Background(), JobWaitRequest{Name: "ghost", WaitSeconds: 30})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *NotFoundError", err, err)
	}
}

// TestJobWaitContextCancellationReturnsPromptly asserts a cancelled context
// returns the context error promptly rather than polling to the cap (the real
// sleep honors ctx; the fake clock here defers to a cancel-aware sleep).
func TestJobWaitContextCancellationReturnsPromptly(t *testing.T) {
	t.Parallel()

	kube := newJobFake(scheduledJob("default", "demo"))
	reach := &fakeReachability{endpoint: Endpoint{BaseURL: "http://127.0.0.1:1"}}
	svc := NewJobService(kube, reach, pendingDashboard(), "default") // real clock + real sleepCtx.

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before the first sleep.

	_, err := svc.Wait(ctx, JobWaitRequest{Name: "demo", Until: "running", WaitSeconds: 30})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled (a cancelled wait must return promptly)", err)
	}
}
