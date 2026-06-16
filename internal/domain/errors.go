package domain

import (
	"errors"
	"fmt"
)

// Error taxonomy (spec §10). Adapters return these typed errors; the domain
// maps them to MCP tool errors with actionable, bounded messages. Raw k8s/Ray
// API errors are never leaked verbatim.
//
// Mapping from adapter failures:
//   - KubeRay adapter: apierrors.IsNotFound → NotFoundError; IsForbidden →
//     ForbiddenError (naming the missing verb/resource); IsConflict → ConflictError
//     (SSA field co-ownership, §7.D); context deadline → TimeoutError.
//   - RayAPI adapter: dial/connection refused, non-2xx that means "head/dashboard
//     not reachable" → RayAPIUnreachableError; 404 on a job id → NotFoundError;
//     context deadline → TimeoutError.
//
// Errors that need to carry context for an actionable message are typed structs
// (NotFound names what is missing; Forbidden names the RBAC verb/resource).
// Timeout is a sentinel because it carries no actionable detail beyond "retry".
// All support errors.Is / errors.As so callers can branch without string-matching.

// ErrTimeout is the sentinel for a deadline/timeout. Adapters wrap it via
// TimeoutError to add operation context while keeping errors.Is(err, ErrTimeout)
// true.
var ErrTimeout = errors.New("operation timed out")

// NotFoundError reports that a named resource (or job id) does not exist. It
// names what was missing so the domain can build an actionable message.
type NotFoundError struct {
	Kind      Kind   // RayCluster/RayJob/RayService, or empty for a Ray-API job id.
	Namespace string // empty for non-namespaced lookups (e.g. a Ray job id).
	Name      string // resource name or Ray submission id.
}

func (e *NotFoundError) Error() string {
	if e.Namespace == "" {
		return fmt.Sprintf("%s %q not found", e.Kind, e.Name)
	}

	return fmt.Sprintf("%s %q not found in namespace %q", e.Kind, e.Name, e.Namespace)
}

// ForbiddenError reports an RBAC denial. It names the missing verb and resource
// so the message can tell the operator exactly which grant to add (spec §10).
type ForbiddenError struct {
	Verb      string // e.g. "get", "list", "patch", "delete".
	Resource  string // e.g. "rayclusters", "rayjobs/status".
	Namespace string // empty for cluster-scoped denials.
}

func (e *ForbiddenError) Error() string {
	if e.Namespace == "" {
		return fmt.Sprintf("forbidden: cannot %s %s (missing RBAC)", e.Verb, e.Resource)
	}

	return fmt.Sprintf("forbidden: cannot %s %s in namespace %q (missing RBAC)", e.Verb, e.Resource, e.Namespace)
}

// ConflictError reports a write conflict — typically Server-Side Apply field
// co-ownership against the Ray autoscaler (spec §7.D), or a resource-version
// conflict. Detail carries the bounded server message.
type ConflictError struct {
	Kind      Kind
	Namespace string
	Name      string
	Detail    string // bounded explanation, e.g. the conflicting field manager.
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict applying %s %q in namespace %q: %s", e.Kind, e.Name, e.Namespace, e.Detail)
}

// RayAPIUnreachableError reports that the Ray dashboard / Job Submission REST
// API could not be reached (dial failure, tunnel dropped, head pod gone). Per
// §10 the domain degrades gracefully: CRD-derived status is still returned,
// annotated that live Ray detail was unavailable and why (the Reason here).
type RayAPIUnreachableError struct {
	Endpoint string // the base URL or tunnel target that was unreachable.
	Reason   string // bounded cause, e.g. "connection refused".
}

func (e *RayAPIUnreachableError) Error() string {
	return fmt.Sprintf("ray api unreachable at %q: %s", e.Endpoint, e.Reason)
}

// TimeoutError wraps ErrTimeout with the operation that timed out so callers can
// both branch on errors.Is(err, ErrTimeout) and surface what was being done.
type TimeoutError struct {
	Op string // the operation that exceeded its deadline, e.g. "JobStatus".
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("%s: %v", e.Op, ErrTimeout)
}

// Unwrap lets errors.Is(err, ErrTimeout) match a TimeoutError.
func (e *TimeoutError) Unwrap() error {
	return ErrTimeout
}
