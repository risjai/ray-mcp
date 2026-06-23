package mcp

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/risjai/ray-mcp/internal/domain"
)

// TestMapDomainErrorRendersFullTaxonomyCleanly is the coverage guard for the one
// taxonomy→tool-error mapper. Every typed domain error must render to exactly its
// own bounded message — both when handed bare and when wrapped by an adapter with
// extra context. The wrapped case is the leak guard: the bounded typed message is
// surfaced, the wrapper's context (here a fake secret) is NOT.
//
// When a new typed error joins the taxonomy (domain/errors.go, domain/merge.go),
// add it here AND to mapDomainError — a member missing from the mapper falls
// through to the default and this test's wrapped case fails for it.
func TestMapDomainErrorRendersFullTaxonomyCleanly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
	}{
		{"NotFound", &domain.NotFoundError{Kind: domain.KindRayCluster, Namespace: "ns", Name: "demo"}},
		{"Forbidden", &domain.ForbiddenError{Verb: "get", Resource: "rayclusters", Namespace: "ns"}},
		{"Conflict", &domain.ConflictError{Kind: domain.KindRayCluster, Namespace: "ns", Name: "demo", Detail: "co-owned by ray-operator"}},
		{"Identity", &domain.IdentityError{Field: "name", Want: "demo", Got: "evil"}},
		{"RayAPIUnreachable", &domain.RayAPIUnreachableError{Endpoint: "http://head:8265", Reason: "connection refused"}},
		{"Timeout", &domain.TimeoutError{Op: "JobStatus"}},
		// ConfirmMismatchError is in the taxonomy (wrong/stale confirm on a destructive op).
		// ConfirmRequiredError is NOT: it is intercepted at the tool layer as a successful
		// preview and never flows through mapDomainError.
		{"ConfirmMismatch", &domain.ConfirmMismatchError{Operation: domain.OpDelete}},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Bare: the mapped error carries exactly the typed bounded message.
			got := mapDomainError(tc.err)
			if got == nil {
				t.Fatal("mapDomainError returned nil for a typed error")
			}
			if got.Error() != tc.err.Error() {
				t.Errorf("bare: mapped message = %q, want the bounded typed message %q", got.Error(), tc.err.Error())
			}

			// Wrapped: an adapter wraps the typed error with extra (potentially
			// sensitive) context via %w. The mapper must extract the bounded typed
			// message and drop the wrapper — no raw context reaches the agent.
			wrapped := fmt.Errorf("adapter context leak-marker-0xDEADBEEF: %w", tc.err)
			gotWrapped := mapDomainError(wrapped)
			if gotWrapped == nil {
				t.Fatal("mapDomainError returned nil for a wrapped typed error")
			}
			if gotWrapped.Error() != tc.err.Error() {
				t.Errorf("wrapped: mapped message = %q, want the bounded typed message %q", gotWrapped.Error(), tc.err.Error())
			}
			if strings.Contains(gotWrapped.Error(), "leak-marker-0xDEADBEEF") {
				t.Errorf("wrapped: mapper leaked the adapter wrapper context: %q", gotWrapped.Error())
			}
		})
	}
}

// TestMapDomainErrorUnknownPassesThrough asserts an error outside the taxonomy is
// returned unchanged (preserving the prior per-family behavior), so the
// consolidation does not silently swallow an unexpected error.
func TestMapDomainErrorUnknownPassesThrough(t *testing.T) {
	t.Parallel()

	err := errors.New("some unmapped error")
	got := mapDomainError(err)
	if got == nil {
		t.Fatal("mapDomainError returned nil for an unmapped error")
	}
	if got.Error() != "some unmapped error" {
		t.Errorf("unmapped error message = %q, want it passed through unchanged", got.Error())
	}
}
