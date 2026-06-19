package observability

import (
	"context"
	"encoding/json"
	"io"
	"sync"

	"github.com/risjai/ray-mcp/internal/domain"
)

// AuditLogger is the mutation audit sink (spec §8, Q8). It satisfies
// domain.AuditSink and writes one JSON line per mutation to an injected
// io.Writer. The destination is the caller's choice — main.go passes os.Stderr
// (the stdio invariant: stdout is the JSON-RPC wire, so audit MUST go to stderr
// or a file, never stdout). Wiring the writer rather than hard-coding os.Stderr
// keeps the package testable (a bytes.Buffer in tests) and lets a future
// --audit-file land without touching the domain.
//
// Records are emitted as newline-delimited JSON (one object per line) so the log
// is greppable and machine-parseable: "what did the agent do?" is answerable by
// reading the stream. A mutex serializes writes so concurrent applies (the HTTP
// transport) never interleave bytes from two records on one line.
type AuditLogger struct {
	mu sync.Mutex
	w  io.Writer
}

// compile-time proof the logger satisfies the domain sink.
var _ domain.AuditSink = (*AuditLogger)(nil)

// NewAuditLogger builds an audit sink writing newline-delimited JSON to w. w must
// not be os.Stdout under the stdio transport (it would corrupt the JSON-RPC
// wire); main.go passes os.Stderr.
func NewAuditLogger(w io.Writer) *AuditLogger {
	return &AuditLogger{w: w}
}

// Record serializes one audit record as a single JSON line. It deliberately
// swallows write/encode errors: an audit destination problem must never mask or
// fail the apply outcome it is recording, and there is no safe second channel to
// report it on under stdio. The encoder appends the trailing newline.
func (l *AuditLogger) Record(_ context.Context, rec domain.AuditRecord) {
	data, err := json.Marshal(rec)
	if err != nil {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	_, _ = l.w.Write(append(data, '\n'))
}
