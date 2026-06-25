// Package rayapi is the read-only adapter for the Ray dashboard / Job Submission
// REST API (the "wedge", spec §5/§7.B). It implements domain.RayAPIPort with
// exactly two reads — JobStatus and JobLogs — and NO submit/stop/delete. That
// absence is the contract: the dashboard is unauthenticated by default (the
// ShadowRay RCE surface), so ray-mcp consumes it read-only and never exposes it
// as a write vector. Every mutation goes through the guarded CRD path instead.
package rayapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
)

// Default log bounds (spec §10: logs are byte/token-bounded, not just
// line-bounded — a tailLines cap alone is insufficient). Applied when LogOptions
// leaves a field zero. The byte ceiling sits in the ~10-20KB band the spec calls
// for; we tail (keep the most recent output), which is what an operator wants.
const (
	defaultTailLines = 200
	defaultMaxBytes  = 16 * 1024
)

// readCeiling caps how many bytes we read off the wire before bounding, as a
// safety net against the unauthenticated dashboard handing back a multi-megabyte
// (or unbounded) buffer over a port-forward tunnel — the /logs endpoint has no
// server-side limit, so this is the only thing protecting client memory. It is
// generously above defaultMaxBytes so a caller can raise MaxBytes well past the
// default and still get a useful tail.
const readCeiling = 4 * 1024 * 1024

// dashboardPathPrefix is the Job Submission REST API base path on port 8265,
// canonical across recent Ray (what `ray job status` / JobSubmissionClient hit).
const dashboardPathPrefix = "/api/jobs/"

// Client implements domain.RayAPIPort against a Ray dashboard endpoint. It holds
// no endpoint of its own — each call takes the domain.Endpoint resolved by
// RayReachability — so one Client serves every cluster. The only retained config
// is the optional dashboard auth header (passed through to an auth proxy, Q6).
//
// Like the other adapters, NewClient touches no network; the first HTTP call is
// the first network I/O.
type Client struct {
	http     *http.Client
	dashAuth string
}

// compile-time check that the adapter satisfies the read-only domain port.
var _ domain.RayAPIPort = (*Client)(nil)

// NewClient builds the adapter WITHOUT touching the network. Per-call deadlines
// come from the caller's context (the kuberay adapter's convention), so the
// http.Client carries no global Timeout — a context deadline drives cancellation
// and maps to a TimeoutError.
func NewClient(cfg *config.Config) *Client {
	return &Client{
		http:     &http.Client{},
		dashAuth: cfg.RayDashboardAuth,
	}
}

// jobDetails is the subset of Ray's JobDetails JSON we consume. Unknown fields
// (job_id, driver_info, runtime_env, …) are ignored by encoding/json. Timestamps
// are epoch MILLISECONDS and nullable, so they decode into *int64 — a null/absent
// value stays nil and yields a zero time.Time rather than the 1970 epoch.
type jobDetails struct {
	SubmissionID string `json:"submission_id"`
	Status       string `json:"status"`
	Message      string `json:"message"`
	StartTime    *int64 `json:"start_time"`
	EndTime      *int64 `json:"end_time"`
}

// logsResponse is Ray's JobLogsResponse: a single field carrying the full
// accumulated log buffer (no server-side tailing or pagination).
type logsResponse struct {
	Logs string `json:"logs"`
}

// JobStatus fetches live job status — GET /api/jobs/{jobID}. The queried jobID is
// carried through as RayJobStatus.JobID rather than echoing the response's
// submission_id, which Ray can return null for a not-yet-started driver.
func (c *Client) JobStatus(ctx context.Context, endpoint domain.Endpoint, jobID string) (domain.RayJobStatus, error) {
	body, err := c.get(ctx, endpoint, jobID, "", "JobStatus")
	if err != nil {
		return domain.RayJobStatus{}, err
	}

	var d jobDetails
	if err := json.Unmarshal(body, &d); err != nil {
		return domain.RayJobStatus{}, &domain.RayAPIUnreachableError{
			Endpoint: endpoint.BaseURL,
			Reason:   fmt.Sprintf("decode job status: %v", err),
		}
	}

	// A 200 with no status field is not a valid JobDetails — most likely an auth
	// proxy's login page or some other interloper answering on 8265 (the
	// --ray-dashboard-auth passthrough makes this plausible). Treat it as
	// unreachable so the wedge degrades to CRD-derived status (§10) rather than
	// surfacing a phantom blank status downstream.
	if d.Status == "" {
		return domain.RayJobStatus{}, &domain.RayAPIUnreachableError{
			Endpoint: endpoint.BaseURL,
			Reason:   "dashboard returned a response with no job status",
		}
	}

	return domain.RayJobStatus{
		JobID:     jobID,
		Status:    d.Status,
		Message:   d.Message,
		StartedAt: millisToTime(d.StartTime),
		EndedAt:   millisToTime(d.EndTime),
	}, nil
}

// JobLogs fetches a bounded log tail — GET /api/jobs/{jobID}/logs. The dashboard
// returns the whole buffer with no server-side limit, so bounding is entirely
// client-side: keep the last TailLines lines, then the trailing MaxBytes bytes,
// reporting how many bytes of the original buffer were dropped (spec §10).
func (c *Client) JobLogs(ctx context.Context, endpoint domain.Endpoint, jobID string, opts domain.LogOptions) (domain.RayJobLogs, error) {
	body, err := c.get(ctx, endpoint, jobID, "logs", "JobLogs")
	if err != nil {
		return domain.RayJobLogs{}, err
	}

	var lr logsResponse
	if err := json.Unmarshal(body, &lr); err != nil {
		return domain.RayJobLogs{}, &domain.RayAPIUnreachableError{
			Endpoint: endpoint.BaseURL,
			Reason:   fmt.Sprintf("decode job logs: %v", err),
		}
	}

	return boundLogs(lr.Logs, opts), nil
}

// get performs the HTTP GET, joins the path, forwards the optional auth header,
// reads a bounded body, and maps transport/status failures to the error taxonomy:
// 404 → NotFoundError (the submission id is unknown to the dashboard); context
// deadline → TimeoutError; dial failure or any other non-2xx → unreachable, so
// the wedge can degrade to CRD-derived status (§10). suffix is "" for status or
// "logs" for the logs sub-resource.
func (c *Client) get(ctx context.Context, endpoint domain.Endpoint, jobID, suffix, op string) ([]byte, error) {
	u, err := jobURL(endpoint.BaseURL, jobID, suffix)
	if err != nil {
		return nil, &domain.RayAPIUnreachableError{Endpoint: endpoint.BaseURL, Reason: err.Error()}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, &domain.RayAPIUnreachableError{Endpoint: endpoint.BaseURL, Reason: err.Error()}
	}
	if c.dashAuth != "" {
		req.Header.Set("Authorization", c.dashAuth)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &domain.TimeoutError{Op: op}
		}
		return nil, &domain.RayAPIUnreachableError{Endpoint: endpoint.BaseURL, Reason: dialReason(err)}
	}
	defer resp.Body.Close() //nolint:errcheck // read-only GET; close error is not actionable.

	if resp.StatusCode == http.StatusNotFound {
		return nil, &domain.NotFoundError{Name: jobID}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &domain.RayAPIUnreachableError{
			Endpoint: endpoint.BaseURL,
			Reason:   fmt.Sprintf("dashboard returned HTTP %d", resp.StatusCode),
		}
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, readCeiling))
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, &domain.TimeoutError{Op: op}
		}
		return nil, &domain.RayAPIUnreachableError{Endpoint: endpoint.BaseURL, Reason: fmt.Sprintf("read response: %v", err)}
	}
	return body, nil
}

// jobURL joins the dashboard base URL with the job path, tolerating a trailing
// slash on the base (so "http://h:8265" and "http://h:8265/" both work) and
// path-escaping the job id.
func jobURL(baseURL, jobID, suffix string) (string, error) {
	base, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse endpoint URL: %w", err)
	}
	p := dashboardPathPrefix + url.PathEscape(jobID)
	if suffix != "" {
		p += "/" + suffix
	}
	ref, err := url.Parse(p)
	if err != nil {
		return "", fmt.Errorf("build job path: %w", err)
	}
	return base.ResolveReference(ref).String(), nil
}

// millisToTime converts an epoch-millisecond pointer to a time.Time, returning
// the zero time when the pointer is nil (Ray sends null for not-yet-started /
// not-yet-ended jobs — that must NOT become the 1970 epoch).
func millisToTime(ms *int64) time.Time {
	if ms == nil {
		return time.Time{}
	}
	return time.UnixMilli(*ms)
}

// dialReason extracts a bounded cause from a transport error for the unreachable
// message, unwrapping a url.Error so the surfaced reason is the underlying cause
// (e.g. "connection refused") rather than the full GET URL.
func dialReason(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) && ue.Err != nil {
		return ue.Err.Error()
	}
	return err.Error()
}

// boundLogs applies the line cap then the byte ceiling to the full buffer, both
// keeping the tail (most recent output), and reports how many bytes of the
// original were dropped. Zero options fall back to the adapter defaults.
func boundLogs(full string, opts domain.LogOptions) domain.RayJobLogs {
	tailLines := opts.TailLines
	if tailLines <= 0 {
		tailLines = defaultTailLines
	}
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxBytes
	}

	out := lastLines(full, tailLines)
	out = lastBytes(out, maxBytes)

	omitted := len(full) - len(out)
	return domain.RayJobLogs{
		Text:         out,
		Truncated:    omitted > 0,
		BytesOmitted: omitted,
	}
}

// lastLines returns the last n lines of s. A trailing newline does not count as
// an extra empty line. n <= 0 returns s unchanged.
func lastLines(s string, n int) string {
	if n <= 0 || s == "" {
		return s
	}
	// Count from the end, skipping a single trailing newline so "a\nb\n" with
	// n=1 yields "b\n", not "".
	end := len(s)
	search := strings.TrimSuffix(s, "\n")
	if strings.Count(search, "\n")+1 <= n {
		return s
	}
	// Find the start of the n-th line from the end.
	count := 0
	for i := len(search) - 1; i >= 0; i-- {
		if search[i] == '\n' {
			count++
			if count == n {
				return s[i+1 : end]
			}
		}
	}
	return s
}

// lastBytes returns the trailing maxBytes bytes of s, backing off to the next
// rune boundary so a multi-byte UTF-8 sequence is never split (the text goes into
// a JSON/MCP payload, which must be valid UTF-8). maxBytes <= 0 returns s.
func lastBytes(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	tail := s[len(s)-maxBytes:]
	// Advance past a partial leading rune so the result starts on a boundary.
	for len(tail) > 0 && !utf8.RuneStart(tail[0]) {
		tail = tail[1:]
	}
	return tail
}
