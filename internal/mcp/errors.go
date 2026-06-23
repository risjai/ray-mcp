package mcp

import (
	"errors"

	"github.com/risjai/ray-mcp/internal/domain"
)

// mapDomainError maps any domain error to a clean, bounded MCP tool error. It is
// the single taxonomy→tool-error policy: every typed domain error carries its own
// actionable, bounded message, so the mapper surfaces THAT (via errors.New, which
// also strips any adapter wrapper context that a %w chain added) rather than the
// raw wrapped string. Errors outside the taxonomy pass through unchanged.
//
// errors.As walks the wrap chain, so a typed error wrapped by an adapter with
// extra context is still matched and rendered to its bounded message — the
// wrapper context never reaches the agent. Adding a typed error to the taxonomy
// (domain/errors.go, domain/merge.go) means adding a case here; the coverage test
// (errors_internal_test.go) fails for any member that falls through.
func mapDomainError(err error) error {
	var notFound *domain.NotFoundError
	if errors.As(err, &notFound) {
		return errors.New(notFound.Error())
	}
	var forbidden *domain.ForbiddenError
	if errors.As(err, &forbidden) {
		return errors.New(forbidden.Error())
	}
	var conflict *domain.ConflictError
	if errors.As(err, &conflict) {
		return errors.New(conflict.Error())
	}
	var identity *domain.IdentityError
	if errors.As(err, &identity) {
		return errors.New(identity.Error())
	}
	var unreachable *domain.RayAPIUnreachableError
	if errors.As(err, &unreachable) {
		return errors.New(unreachable.Error())
	}
	var timeout *domain.TimeoutError
	if errors.As(err, &timeout) {
		return errors.New(timeout.Error())
	}
	var mismatch *domain.ConfirmMismatchError
	if errors.As(err, &mismatch) {
		return errors.New(mismatch.Error())
	}
	return err
}
