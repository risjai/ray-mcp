package rayapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// newTestClient builds a Client whose only relevant config is the optional
// dashboard auth header. NewClient touches no network, so this is offline-safe.
func newTestClient(dashAuth string) *Client {
	return NewClient(&config.Config{RayDashboardAuth: dashAuth})
}

// endpointFor wraps an httptest server URL as the domain.Endpoint the port takes.
func endpointFor(srv *httptest.Server) domain.Endpoint {
	return domain.Endpoint{BaseURL: srv.URL}
}

// writeBody writes a response body from a test handler, failing the test on a
// write error. It exists so the handlers satisfy errcheck without scattering
// `_, _ =` across every fixture.
func writeBody(t *testing.T, w http.ResponseWriter, format string, args ...any) {
	t.Helper()
	if _, err := fmt.Fprintf(w, format, args...); err != nil {
		t.Errorf("write response body: %v", err)
	}
}

// TestReadOnlyByConstruction asserts the RayAPIPort contract exposes ONLY the two
// read methods (spec §5/§7.B/Q6: the dashboard is unauthenticated/RCE-capable, so
// it is consumed read-only and the absence of submit/stop IS the contract). A new
// write method on the port fails this test — a compile-time-adjacent guard.
func TestReadOnlyByConstruction(t *testing.T) {
	var got []string
	pt := reflect.TypeOf((*domain.RayAPIPort)(nil)).Elem()
	for i := range pt.NumMethod() {
		got = append(got, pt.Method(i).Name)
	}
	sort.Strings(got)

	want := []string{"JobLogs", "JobStatus"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RayAPIPort methods = %v, want exactly %v (no write methods allowed)", got, want)
	}
}

func TestJobStatusHappyPath(t *testing.T) {
	const jobID = "raysubmit_abc123"
	// start_time / end_time are epoch MILLISECONDS in the Ray contract.
	const startMillis = 1_700_000_000_000
	const endMillis = 1_700_000_050_000

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if want := "/api/jobs/" + jobID; r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
		writeBody(t, w, `{
			"type": "SUBMISSION",
			"submission_id": %q,
			"job_id": "01000000",
			"status": "RUNNING",
			"message": "Job is running",
			"start_time": %d,
			"end_time": %d
		}`, jobID, startMillis, endMillis)
	}))
	defer srv.Close()

	st, err := newTestClient("").JobStatus(context.Background(), endpointFor(srv), jobID)
	if err != nil {
		t.Fatalf("JobStatus: unexpected error: %v", err)
	}
	if st.JobID != jobID {
		t.Errorf("JobID = %q, want %q (the queried submission id)", st.JobID, jobID)
	}
	if st.Status != "RUNNING" {
		t.Errorf("Status = %q, want RUNNING", st.Status)
	}
	if st.Message != "Job is running" {
		t.Errorf("Message = %q, want %q", st.Message, "Job is running")
	}
	if !st.StartedAt.Equal(time.UnixMilli(startMillis)) {
		t.Errorf("StartedAt = %v, want %v", st.StartedAt, time.UnixMilli(startMillis))
	}
	if !st.EndedAt.Equal(time.UnixMilli(endMillis)) {
		t.Errorf("EndedAt = %v, want %v", st.EndedAt, time.UnixMilli(endMillis))
	}
}

// TestJobStatusNullTimestampsAreZero asserts a null/absent start_time|end_time
// yields a zero time.Time — NOT the 1970 epoch a naive time.Unix(0) would give.
func TestJobStatusNullTimestampsAreZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBody(t, w, `{"submission_id":"s","status":"PENDING","message":null,"start_time":null,"end_time":null}`)
	}))
	defer srv.Close()

	st, err := newTestClient("").JobStatus(context.Background(), endpointFor(srv), "s")
	if err != nil {
		t.Fatalf("JobStatus: unexpected error: %v", err)
	}
	if !st.StartedAt.IsZero() {
		t.Errorf("StartedAt = %v, want zero time (null start_time must not become 1970)", st.StartedAt)
	}
	if !st.EndedAt.IsZero() {
		t.Errorf("EndedAt = %v, want zero time", st.EndedAt)
	}
	if st.Message != "" {
		t.Errorf("Message = %q, want empty for null message", st.Message)
	}
}

// TestJobStatusNotFoundMapsTyped asserts a 404 (Ray returns plain text, not JSON)
// maps to a typed *domain.NotFoundError naming the submission id — the likely
// real-world miss when status.jobId is set but the dashboard doesn't know it.
func TestJobStatusNotFoundMapsTyped(t *testing.T) {
	const jobID = "raysubmit_missing"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeBody(t, w, "Job %s does not exist", jobID)
	}))
	defer srv.Close()

	_, err := newTestClient("").JobStatus(context.Background(), endpointFor(srv), jobID)
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *domain.NotFoundError", err, err)
	}
	if nf.Name != jobID {
		t.Errorf("NotFoundError.Name = %q, want %q", nf.Name, jobID)
	}
}

// TestJobStatusBlankStatusDegrades asserts a 200 carrying valid JSON with no
// status field (e.g. an auth-proxy login page answering on 8265) maps to
// RayAPIUnreachableError so the wedge degrades to CRD status rather than
// surfacing a phantom blank status.
func TestJobStatusBlankStatusDegrades(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBody(t, w, `{"not":"a job details object"}`)
	}))
	defer srv.Close()

	_, err := newTestClient("").JobStatus(context.Background(), endpointFor(srv), "s")
	var ue *domain.RayAPIUnreachableError
	if !errors.As(err, &ue) {
		t.Fatalf("error = %T (%v), want *domain.RayAPIUnreachableError", err, err)
	}
}

// TestJobStatusServerErrorUnreachable asserts a non-2xx that is not a 404 (e.g.
// 500) maps to RayAPIUnreachableError so the wedge can degrade to CRD status.
func TestJobStatusServerErrorUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		writeBody(t, w, "boom")
	}))
	defer srv.Close()

	_, err := newTestClient("").JobStatus(context.Background(), endpointFor(srv), "s")
	var ue *domain.RayAPIUnreachableError
	if !errors.As(err, &ue) {
		t.Fatalf("error = %T (%v), want *domain.RayAPIUnreachableError", err, err)
	}
}

// TestJobStatusDialFailureUnreachable points at a closed server so the dial fails.
func TestJobStatusDialFailureUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close() // close immediately so the port refuses the connection.

	_, err := newTestClient("").JobStatus(context.Background(), domain.Endpoint{BaseURL: url}, "s")
	var ue *domain.RayAPIUnreachableError
	if !errors.As(err, &ue) {
		t.Fatalf("error = %T (%v), want *domain.RayAPIUnreachableError", err, err)
	}
}

// TestJobStatusContextDeadlineMapsTimeout asserts a context deadline maps to a
// TimeoutError that errors.Is(ErrTimeout) — matching the kuberay adapter taxonomy.
func TestJobStatusContextDeadlineMapsTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release // block until the test's deadline fires.
		writeBody(t, w, `{"submission_id":"s","status":"RUNNING"}`)
	}))
	defer srv.Close()
	defer close(release)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := newTestClient("").JobStatus(ctx, endpointFor(srv), "s")
	if !errors.Is(err, domain.ErrTimeout) {
		t.Fatalf("error = %T (%v), want errors.Is(ErrTimeout)", err, err)
	}
}

func TestJobLogsHappyPath(t *testing.T) {
	const jobID = "raysubmit_logs"
	const body = "line one\nline two\nline three\n"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if want := "/api/jobs/" + jobID + "/logs"; r.URL.Path != want {
			t.Errorf("path = %q, want %q", r.URL.Path, want)
		}
		writeBody(t, w, `{"logs":%q}`, body)
	}))
	defer srv.Close()

	logs, err := newTestClient("").JobLogs(context.Background(), endpointFor(srv), jobID, domain.LogOptions{})
	if err != nil {
		t.Fatalf("JobLogs: unexpected error: %v", err)
	}
	if logs.Text != body {
		t.Errorf("Text = %q, want %q", logs.Text, body)
	}
	if logs.Truncated {
		t.Errorf("Truncated = true, want false for a small buffer")
	}
	if logs.BytesOmitted != 0 {
		t.Errorf("BytesOmitted = %d, want 0", logs.BytesOmitted)
	}
}

// TestJobLogsByteCeilingTails asserts the byte ceiling keeps the TAIL (most recent
// output) and reports the omitted-byte count against the full buffer (spec §10).
func TestJobLogsByteCeilingTails(t *testing.T) {
	full := strings.Repeat("x", 5000)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBody(t, w, `{"logs":%q}`, full)
	}))
	defer srv.Close()

	const maxBytes = 1000
	logs, err := newTestClient("").JobLogs(context.Background(), endpointFor(srv), "s",
		domain.LogOptions{MaxBytes: maxBytes})
	if err != nil {
		t.Fatalf("JobLogs: unexpected error: %v", err)
	}
	if !logs.Truncated {
		t.Fatalf("Truncated = false, want true when buffer exceeds the ceiling")
	}
	if len(logs.Text) > maxBytes {
		t.Errorf("len(Text) = %d, want <= %d", len(logs.Text), maxBytes)
	}
	if logs.BytesOmitted != len(full)-len(logs.Text) {
		t.Errorf("BytesOmitted = %d, want %d", logs.BytesOmitted, len(full)-len(logs.Text))
	}
	// Kept text must be the SUFFIX (tail) of the original buffer.
	if !strings.HasSuffix(full, logs.Text) {
		t.Errorf("Text is not a suffix of the full buffer; expected the tail")
	}
}

// TestJobLogsTailLines asserts the line cap keeps the last N lines.
func TestJobLogsTailLines(t *testing.T) {
	var sb strings.Builder
	for i := range 50 {
		fmt.Fprintf(&sb, "line %d\n", i)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBody(t, w, `{"logs":%q}`, sb.String())
	}))
	defer srv.Close()

	logs, err := newTestClient("").JobLogs(context.Background(), endpointFor(srv), "s",
		domain.LogOptions{TailLines: 3, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("JobLogs: unexpected error: %v", err)
	}
	if !strings.Contains(logs.Text, "line 49") || !strings.Contains(logs.Text, "line 47") {
		t.Errorf("Text = %q, want the last 3 lines (47..49)", logs.Text)
	}
	if strings.Contains(logs.Text, "line 46") {
		t.Errorf("Text = %q, want only the last 3 lines (line 46 should be dropped)", logs.Text)
	}
	if !logs.Truncated {
		t.Errorf("Truncated = false, want true when earlier lines were dropped")
	}
}

// TestJobLogsByteCeilingKeepsValidUTF8 asserts byte-tailing never splits a
// multi-byte rune (the Text goes into a JSON/MCP payload, which must be valid UTF-8).
func TestJobLogsByteCeilingKeepsValidUTF8(t *testing.T) {
	full := strings.Repeat("世界", 1000) // 3 bytes/rune; ceiling will land mid-rune.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBody(t, w, `{"logs":%q}`, full)
	}))
	defer srv.Close()

	logs, err := newTestClient("").JobLogs(context.Background(), endpointFor(srv), "s",
		domain.LogOptions{MaxBytes: 1000}) // 1000 is not a multiple of 3.
	if err != nil {
		t.Fatalf("JobLogs: unexpected error: %v", err)
	}
	if !utf8.ValidString(logs.Text) {
		t.Errorf("Text is not valid UTF-8 after byte truncation")
	}
}

// TestJobLogsDefaultByteCeilingApplied asserts opts{} (zero) still bounds a huge
// buffer via the adapter default ceiling (LogOptions doc: 0 = adapter default).
func TestJobLogsDefaultByteCeilingApplied(t *testing.T) {
	full := strings.Repeat("y", 200*1024) // 200 KB, well over any ~10-20KB default.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeBody(t, w, `{"logs":%q}`, full)
	}))
	defer srv.Close()

	logs, err := newTestClient("").JobLogs(context.Background(), endpointFor(srv), "s", domain.LogOptions{})
	if err != nil {
		t.Fatalf("JobLogs: unexpected error: %v", err)
	}
	if !logs.Truncated {
		t.Fatalf("Truncated = false, want true (default ceiling must bound a 200KB buffer)")
	}
	if len(logs.Text) >= len(full) {
		t.Errorf("len(Text) = %d, want bounded below the 200KB buffer", len(logs.Text))
	}
}

// TestJobLogsNotFoundMapsTyped asserts a 404 on the logs path maps to NotFound too.
func TestJobLogsNotFoundMapsTyped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		writeBody(t, w, "Job s does not exist")
	}))
	defer srv.Close()

	_, err := newTestClient("").JobLogs(context.Background(), endpointFor(srv), "s", domain.LogOptions{})
	var nf *domain.NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("error = %T (%v), want *domain.NotFoundError", err, err)
	}
}

// TestDashboardAuthForwarded asserts a configured --ray-dashboard-auth value is
// sent verbatim as the Authorization header (auth-proxy passthrough, Q6).
func TestDashboardAuthForwarded(t *testing.T) {
	const authVal = "Bearer secret-token"
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		writeBody(t, w, `{"submission_id":"s","status":"RUNNING"}`)
	}))
	defer srv.Close()

	if _, err := newTestClient(authVal).JobStatus(context.Background(), endpointFor(srv), "s"); err != nil {
		t.Fatalf("JobStatus: unexpected error: %v", err)
	}
	if gotAuth != authVal {
		t.Errorf("Authorization header = %q, want %q", gotAuth, authVal)
	}
}

// TestNoAuthHeaderWhenUnset asserts no Authorization header leaks when unconfigured.
func TestNoAuthHeaderWhenUnset(t *testing.T) {
	var hadAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, hadAuth = r.Header["Authorization"]
		writeBody(t, w, `{"submission_id":"s","status":"RUNNING"}`)
	}))
	defer srv.Close()

	if _, err := newTestClient("").JobStatus(context.Background(), endpointFor(srv), "s"); err != nil {
		t.Fatalf("JobStatus: unexpected error: %v", err)
	}
	if hadAuth {
		t.Errorf("Authorization header present, want none when --ray-dashboard-auth is unset")
	}
}

// TestBaseURLTrailingSlashTolerated asserts URL joining is correct whether or not
// the endpoint base URL carries a trailing slash.
func TestBaseURLTrailingSlashTolerated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/jobs/s" {
			t.Errorf("path = %q, want /api/jobs/s (double-slash leaked?)", r.URL.Path)
		}
		writeBody(t, w, `{"submission_id":"s","status":"RUNNING"}`)
	}))
	defer srv.Close()

	if _, err := newTestClient("").JobStatus(context.Background(),
		domain.Endpoint{BaseURL: srv.URL + "/"}, "s"); err != nil {
		t.Fatalf("JobStatus: unexpected error: %v", err)
	}
}

// TestLastLines pins the trailing-newline-skip and boundary edge cases the
// lastLines comment documents: a trailing newline does not count as an extra
// empty line, and n >= line count returns the input unchanged.
func TestLastLines(t *testing.T) {
	tests := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"trailing newline, n=1", "a\nb\n", 1, "b\n"},
		{"trailing newline, n=2", "a\nb\n", 2, "a\nb\n"},
		{"no trailing newline, n=1", "a\nb", 1, "b"},
		{"n exceeds line count", "a\nb\n", 5, "a\nb\n"},
		{"n equals line count", "a\nb\nc\n", 3, "a\nb\nc\n"},
		{"keep last 2 of 3", "a\nb\nc\n", 2, "b\nc\n"},
		{"empty string", "", 3, ""},
		{"single line no newline", "only", 1, "only"},
		{"n=0 returns unchanged", "a\nb\n", 0, "a\nb\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := lastLines(tc.in, tc.n); got != tc.want {
				t.Errorf("lastLines(%q, %d) = %q, want %q", tc.in, tc.n, got, tc.want)
			}
		})
	}
}
