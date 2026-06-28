package mcp_test

import (
	"context"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/risjai/ray-mcp/internal/config"
	"github.com/risjai/ray-mcp/internal/domain"
	mcpserver "github.com/risjai/ray-mcp/internal/mcp"
)

// listableJob builds a JobDetail whose embedded summary carries both status
// fields ray_job_list surfaces.
func listableJob(namespace, name, jobStatus, deploymentStatus string) domain.JobDetail {
	return domain.JobDetail{
		JobSummary: domain.JobSummary{
			Name:                name,
			Namespace:           namespace,
			JobStatus:           jobStatus,
			JobDeploymentStatus: deploymentStatus,
			Health:              "h",
		},
	}
}

// TestJobListToolRegisteredReadOnly asserts ray_job_list is registered, alongside
// the other wedge tools, and annotated read-only.
func TestJobListToolRegisteredReadOnly(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "default"}
	session := connectJobs(t, cfg, wedgeFor(map[string]domain.JobDetail{}, fakeRayAPI{}))

	res, err := session.ListTools(context.Background(), &mcp.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools: %v", err)
	}
	var tool *mcp.Tool
	for _, tl := range res.Tools {
		if tl.Name == "ray_job_list" {
			tool = tl
		}
	}
	if tool == nil {
		t.Fatalf("ray_job_list not registered; got %v", res.Tools)
	}
	if tool.Annotations == nil || !tool.Annotations.ReadOnlyHint {
		t.Errorf("ray_job_list missing readOnlyHint annotation: %+v", tool.Annotations)
	}
}

// TestJobListRowsCarryBothStatuses is the spec's headline requirement at the MCP
// edge: each row surfaces BOTH the Ray-side jobStatus and the CRD
// jobDeploymentStatus.
func TestJobListRowsCarryBothStatuses(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	jobs := map[string]domain.JobDetail{
		"demo/trainer": listableJob("demo", "trainer", "RUNNING", "Running"),
	}
	session := connectJobs(t, cfg, wedgeFor(jobs, fakeRayAPI{}))

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_list",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	rows, ok := sc["jobs"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("jobs = %v, want exactly one row", sc["jobs"])
	}
	row := rows[0].(map[string]any)
	if row["jobStatus"] != "RUNNING" {
		t.Errorf("row.jobStatus = %v, want RUNNING", row["jobStatus"])
	}
	if row["deploymentStatus"] != "Running" {
		t.Errorf("row.deploymentStatus = %v, want Running", row["deploymentStatus"])
	}
	if sc["count"] != float64(1) {
		t.Errorf("count = %v, want 1", sc["count"])
	}
}

// TestJobListMoreAvailableSurfacesContinue asserts the pagination contract: a
// continue token from the backend surfaces as moreAvailable=true + continue, with
// the text summary saying "more available".
func TestJobListMoreAvailableSurfacesContinue(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{DefaultNamespace: "demo"}
	reader := &fakeJobReader{
		jobs:         map[string]domain.JobDetail{"demo/a": listableJob("demo", "a", "RUNNING", "Running")},
		listContinue: "tok-next",
	}
	session := connectJobs(t, cfg, mcpserver.WedgeBackend{
		Jobs:  reader,
		Reach: fakeReach{endpoint: domain.Endpoint{BaseURL: "http://127.0.0.1:30000"}},
		API:   fakeRayAPI{},
	})

	res, err := session.CallTool(context.Background(), &mcp.CallToolParams{
		Name:      "ray_job_list",
		Arguments: map[string]any{},
	})
	if err != nil {
		t.Fatalf("CallTool: %v", err)
	}
	if res.IsError {
		t.Fatalf("tool reported error: %+v", res)
	}
	sc := res.StructuredContent.(map[string]any)
	if sc["moreAvailable"] != true {
		t.Errorf("moreAvailable = %v, want true", sc["moreAvailable"])
	}
	if sc["continue"] != "tok-next" {
		t.Errorf("continue = %v, want tok-next", sc["continue"])
	}
	if len(res.Content) == 0 {
		t.Fatalf("no text content")
	}
	text := res.Content[0].(*mcp.TextContent).Text
	if !strings.Contains(text, "more available") {
		t.Errorf("summary %q does not mention 'more available'", text)
	}
}
