package domain

import "context"

// The cluster-touching half of the unified apply pipeline (spec §7.C, steps
// 6-7). Task 8a built the pure merge/diff core (merge.go, diff.go); this file
// wraps it with the orchestration that talks to a cluster through the Applier
// port: always DryRunAll first (the validation oracle — Q4/B2: every apply runs
// a server-side dry-run regardless of the caller's dryRun flag), then either
// stop (dryRun) or SSA-apply for real and diff the read-back against intent.
// Every apply emits an audit record at this single choke point (F14), so every
// create/update/scale/deploy tool built on top is audited from birth.
//
// It imports no Kubernetes/HTTP packages: the Applier port and the AuditSink are
// interfaces the adapters satisfy, and the spec crosses the boundary as the
// unstructured MergedSpec from Task 8a.

// ApplyOptions controls one Server-Side Apply (Task 8b + Task 10).
type ApplyOptions struct {
	// DryRun maps to DryRunAll: the API server validates against the installed CRD
	// schema (rejecting unknown fields and invalid values) but persists nothing.
	DryRun bool
	// Force takes ownership of fields another manager owns (SSA ForceOwnership). It
	// is the update/scale conflict-retry knob: ray-mcp re-applies once with force
	// after a genuine field-ownership conflict, having re-read the live object so it
	// re-asserts (never clobbers) the contended value — notably the autoscaler's
	// worker replicas on the atomic workerGroupSpecs list (Task 10). create never
	// forces.
	Force bool
}

// Applier is the narrow write slice of KubeRayPort the apply pipeline needs: a
// single Server-Side Apply parameterized by Kind, with DryRun/Force options.
// ApplyService depends on this (not the full port) so it is unit-testable with a
// fake that records what it was asked to apply. The KubeRay adapter satisfies it;
// the full KubeRayPort satisfies it too.
type Applier interface {
	// Apply server-side-applies the merged unstructured spec for kind/namespace/
	// name with the ray-mcp field manager and returns the server's view of the
	// object (the read-back). opts controls DryRunAll and ForceOwnership.
	Apply(ctx context.Context, kind Kind, namespace, name string, spec MergedSpec, opts ApplyOptions) (MergedSpec, error)
}

// ApplyRequest is one mutation through the pipeline: the already-merged
// unstructured spec (built by the tool layer via Merge — Task 8a), its identity,
// the dryRun flag, and the tool name + bounded args summary for the audit record.
// The spec is expected to already carry metadata.name/namespace equal to the
// identity (Merge enforces that); the pipeline passes them to the Applier and
// uses the spec subtree to scope the diff.
type ApplyRequest struct {
	Kind      Kind
	Namespace string
	Name      string
	Spec      MergedSpec // the merged intent (curated base + rawSpec), from Merge.
	DryRun    bool
	// Force maps to SSA ForceOwnership on the commit apply. create leaves it false;
	// update/scale set it on the conflict-retry apply only (Task 10). The DryRunAll
	// preview is never forced — force changes ownership, not validation.
	Force bool

	// Tool and ArgsSummary feed the audit record (spec §8, Q8). ArgsSummary is a
	// short, already-bounded description of the call — never the full spec.
	Tool        string
	ArgsSummary string
}

// ApplyResult is the outcome of an apply: the server's read-back object, the
// summarized diff of intent-vs-result (spec §10), and whether this was a dry-run.
//
// Note on pruning (Q4): the spec anticipated surfacing fields the API server
// *silently prunes*. That premise holds for the legacy create/update path, but
// NOT for Server-Side Apply (Q3/Q5, the path this pipeline uses): an SSA against
// a structural-schema CRD is validated server-side against the INSTALLED CRD
// schema and REJECTS an unknown field with a hard error (`field not declared in
// schema`) rather than pruning it. (The adapter sends the raw unstructured map,
// so this is the installed schema's verdict, not our compiled baseline's — the
// wedge holds: fields newer than our pin still reach the server.) The
// unconditional DryRunAll below is therefore the validation gate that surfaces
// such an error loudly, before any commit — strictly better than silent pruning.
// Reporting unknown-field rejection as an actionable tool error is owned by the
// create tool (Task 9), not this pipeline; that task should say "rejected as an
// undeclared field", NOT "pruned" — the field never silently disappears.
type ApplyResult struct {
	Object MergedSpec // the server's view (dry-run result, or the post-apply read-back).
	Diff   DiffResult // summarized change from the submitted intent to Object.
	DryRun bool       // true if nothing was persisted.
}

// ApplyService is the orchestration layer over the Applier for every mutating
// CRD tool. It owns the cross-cutting write policy the tool layer must not
// duplicate: the unconditional pre-apply DryRunAll, the dryRun short-circuit, the
// intent-vs-result diff, and the audit emission. It imports no Kubernetes or HTTP
// packages — only the ports and the DTOs.
type ApplyService struct {
	kube  Applier
	audit AuditSink
}

// NewApplyService builds the service over an Applier and an AuditSink. The audit
// sink is required by the invariant (audit every mutation); a nil sink is
// replaced with a no-op so the pipeline never holds a nil and the invariant
// cannot be silently skipped by passing nil.
func NewApplyService(kube Applier, audit AuditSink) *ApplyService {
	if audit == nil {
		audit = NopAuditSink{}
	}
	return &ApplyService{kube: kube, audit: audit}
}

// Apply runs the cluster-touching pipeline for one already-merged spec:
//
//  1. ALWAYS DryRunAll the spec — the API server validates it against the
//     installed CRD schema (rejecting unknown fields and invalid values) with no
//     mutation (spec §7.C step 6; Q4/B2: the dry-run is the validation oracle on
//     every apply).
//  2. If req.DryRun: return the dry-run object + the intent-vs-dry-run diff.
//     Nothing was persisted.
//  3. Otherwise SSA-apply for real, read back, and diff intent-vs-read-back
//     (step 7).
//
// Every path emits exactly one audit record (success or failure) before
// returning. A failure in the dry-run phase is the cheap, non-mutating validation
// and is returned as-is, audited as a failed dry-run.
func (s *ApplyService) Apply(ctx context.Context, req ApplyRequest) (ApplyResult, error) {
	// Step 1: unconditional DryRunAll. Validates against the installed CRD schema
	// (rejecting unknown fields and invalid values) without persisting — on a
	// committing apply just as on a preview. The preview inherits req.Force: SSA
	// dry-run also performs field-ownership conflict detection, so a forced commit
	// (the update/scale conflict-retry) must be previewed WITH force, or the dry-run
	// would re-conflict before the commit it is meant to validate.
	dryRunObj, err := s.kube.Apply(ctx, req.Kind, req.Namespace, req.Name, req.Spec, ApplyOptions{DryRun: true, Force: req.Force})
	if err != nil {
		s.emit(ctx, req, true, auditFailure(err))
		return ApplyResult{}, err
	}

	if req.DryRun {
		s.emit(ctx, req, true, auditSuccess())
		return ApplyResult{
			Object: dryRunObj,
			Diff:   diffSpec(req.Spec, dryRunObj),
			DryRun: true,
		}, nil
	}

	// Step 7: commit via SSA, then diff the server read-back against intent.
	applied, err := s.kube.Apply(ctx, req.Kind, req.Namespace, req.Name, req.Spec, ApplyOptions{Force: req.Force})
	if err != nil {
		s.emit(ctx, req, false, auditFailure(err))
		return ApplyResult{}, err
	}

	s.emit(ctx, req, false, auditSuccess())
	return ApplyResult{
		Object: applied,
		Diff:   diffSpec(req.Spec, applied),
		DryRun: false,
	}, nil
}

// emit writes one audit record for an apply attempt, filling caller identity from
// the context (the transport sets it; stdio leaves the default). It never returns
// an error: a failed audit write must not mask the apply outcome, and the sink
// itself owns its destination (stderr/file, never stdout).
func (s *ApplyService) emit(ctx context.Context, req ApplyRequest, dryRun bool, o auditOutcome) {
	s.audit.Record(ctx, AuditRecord{
		Caller:      CallerFromContext(ctx),
		Tool:        req.Tool,
		Kind:        req.Kind,
		Namespace:   req.Namespace,
		Name:        req.Name,
		ArgsSummary: req.ArgsSummary,
		DryRun:      dryRun,
		Outcome:     o.outcome,
		Error:       o.errMsg,
	})
}

// diffSpec summarizes the change between the submitted intent and the server's
// view, scoped to the spec subtree. We diff only spec, not the whole object: the
// server populates metadata (uid, resourceVersion, managedFields, creation
// timestamp) and status, which are not the caller's intent and would swamp the
// summary with server-authored noise. Both sides fall back to the whole object
// when they carry no spec, so a malformed input still produces a bounded diff
// rather than silently empty.
func diffSpec(intent, result MergedSpec) DiffResult {
	before := subtreeOrWhole(intent, "spec")
	after := subtreeOrWhole(result, "spec")
	return Diff(before, after, DefaultDiffMaxDepth)
}

// subtreeOrWhole returns m[key] as a MergedSpec when it is an object, else m
// itself. It lets the diff focus on spec while degrading gracefully if the key
// is absent.
func subtreeOrWhole(m MergedSpec, key string) MergedSpec {
	if sub, ok := m[key].(map[string]any); ok {
		return sub
	}
	return m
}

// auditOutcome is the internal result-shape emit consumes, so the success/failure
// call sites stay symmetric.
type auditOutcome struct {
	outcome string
	errMsg  string
}

func auditSuccess() auditOutcome {
	return auditOutcome{outcome: AuditOutcomeSuccess}
}

func auditFailure(err error) auditOutcome {
	return auditOutcome{outcome: AuditOutcomeFailure, errMsg: err.Error()}
}

// Audit outcome values for an audit record.
const (
	// AuditOutcomeSuccess marks an apply that completed (dry-run or committed).
	AuditOutcomeSuccess = "success"
	// AuditOutcomeFailure marks an apply that errored.
	AuditOutcomeFailure = "failure"
)

// callerContextKey is the unexported context key under which the transport
// stashes the resolved caller identity. Unexported so only this package can
// set/read it — callers go through WithCaller / CallerFromContext.
type callerContextKey struct{}

// DefaultCaller is the caller identity recorded when none was set on the context
// — the stdio case, where there is no HTTP token to attribute (spec §8: under
// stdio the audit caller is "local/stdio"; Task 24 fills HTTP identities).
const DefaultCaller = "local/stdio"

// WithCaller returns a context carrying the resolved caller identity for audit
// records. The transport layer sets it (static-token fingerprint, or the
// TokenReview SA username in Task 24); the domain only reads it.
func WithCaller(ctx context.Context, caller string) context.Context {
	return context.WithValue(ctx, callerContextKey{}, caller)
}

// CallerFromContext returns the caller identity set by WithCaller, or
// DefaultCaller when none is present (stdio, or any path that did not set one).
func CallerFromContext(ctx context.Context) string {
	if c, ok := ctx.Value(callerContextKey{}).(string); ok && c != "" {
		return c
	}
	return DefaultCaller
}

// AuditRecord is one mutation audit entry (spec §8, Q8): who did what, whether it
// was a dry-run, and the outcome. It is the structured shape the AuditSink
// serializes; the domain builds it at the apply choke point so every mutation is
// audited uniformly. Fields are bounded by construction (ArgsSummary is a short
// summary, Error is the bounded domain error message) — never the full spec or a
// raw stack.
type AuditRecord struct {
	Caller      string `json:"caller"`
	Tool        string `json:"tool"`
	Kind        Kind   `json:"kind"`
	Namespace   string `json:"namespace"`
	Name        string `json:"name"`
	ArgsSummary string `json:"argsSummary,omitempty"`
	DryRun      bool   `json:"dryRun"`
	Outcome     string `json:"outcome"` // AuditOutcomeSuccess | AuditOutcomeFailure.
	Error       string `json:"error,omitempty"`
}

// AuditSink receives one AuditRecord per mutation. The observability package
// implements it (JSON to stderr or a file); the domain depends only on this
// interface, so the audit destination is an edge concern and the hexagonal
// invariant holds. Record must never write to stdout (the stdio JSON-RPC wire).
type AuditSink interface {
	Record(ctx context.Context, rec AuditRecord)
}

// NopAuditSink is an AuditSink that discards records. It is the safe default when
// no real sink is wired (so NewApplyService never holds a nil sink) and is useful
// in tests that do not assert on audit output.
type NopAuditSink struct{}

// Record implements AuditSink by doing nothing.
func (NopAuditSink) Record(context.Context, AuditRecord) {}
