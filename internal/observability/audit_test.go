package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/risjai/ray-mcp/internal/domain"
	"github.com/risjai/ray-mcp/internal/observability"
)

// TestAuditLoggerWritesJSONLine asserts one record serializes to exactly one
// newline-terminated JSON object carrying the audit fields (caller, tool,
// target, dryRun, outcome).
func TestAuditLoggerWritesJSONLine(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := observability.NewAuditLogger(&buf)

	log.Record(context.Background(), domain.AuditRecord{
		Caller:    "local/stdio",
		Tool:      "ray_cluster_create",
		Kind:      domain.KindRayCluster,
		Namespace: "ray",
		Name:      "demo",
		DryRun:    false,
		Outcome:   domain.AuditOutcomeSuccess,
	})

	out := buf.String()
	if strings.Count(out, "\n") != 1 || !strings.HasSuffix(out, "\n") {
		t.Fatalf("want exactly one newline-terminated line, got %q", out)
	}

	var got domain.AuditRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(out)), &got); err != nil {
		t.Fatalf("audit line is not valid JSON: %v (%q)", err, out)
	}
	if got.Tool != "ray_cluster_create" || got.Kind != domain.KindRayCluster {
		t.Errorf("round-trip = %+v, want the create record", got)
	}
	if got.Namespace != "ray" || got.Name != "demo" {
		t.Errorf("round-trip target = %s/%s, want ray/demo", got.Namespace, got.Name)
	}
	if got.Outcome != domain.AuditOutcomeSuccess {
		t.Errorf("round-trip Outcome = %q, want success", got.Outcome)
	}
}

// TestAuditLoggerSatisfiesSinkAndIsStdoutFree is a guard documenting intent: the
// logger writes only to the injected writer, never os.Stdout. We prove it
// structurally — the only sink is the writer we pass — by writing to a buffer and
// confirming the bytes landed there. (The process-global stdout-clean assertion
// for the whole server lives in the mcp package's stdout invariant test.)
func TestAuditLoggerSatisfiesSinkAndIsStdoutFree(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	var sink domain.AuditSink = observability.NewAuditLogger(&buf)

	sink.Record(context.Background(), domain.AuditRecord{Tool: "ray_cluster_update", Outcome: domain.AuditOutcomeSuccess})

	if buf.Len() == 0 {
		t.Fatal("record did not reach the injected writer")
	}
}

// TestAuditLoggerConcurrentWritesDoNotInterleave asserts the mutex keeps
// concurrent records on whole, separable lines — every line must parse as one
// complete JSON object, with no torn writes.
func TestAuditLoggerConcurrentWritesDoNotInterleave(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	log := observability.NewAuditLogger(&buf)

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			log.Record(context.Background(), domain.AuditRecord{
				Tool:    "ray_cluster_create",
				Name:    "demo",
				Outcome: domain.AuditOutcomeSuccess,
			})
		}()
	}
	wg.Wait()

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != n {
		t.Fatalf("got %d lines, want %d (one per record, no torn/merged writes)", len(lines), n)
	}
	for _, line := range lines {
		var rec domain.AuditRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("a concurrent line did not parse as one JSON object: %v (%q)", err, line)
		}
	}
}
