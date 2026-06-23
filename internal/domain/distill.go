package domain

import "strings"

// Status distillation — the pure, kind-agnostic core (design note
// docs/specs/ray-mcp-distillation-design.md §6). Distillation turns a CRD's
// typed status (± live Ray API status) into one bounded, agent-actionable line
// — "Pending: unschedulable, no GPU nodes" instead of a status blob. This file
// owns the genuinely shared, I/O-free half: composing the line and formatting a
// condition's reason/message. The per-kind *extraction* (which condition, the
// wedge predicate) reads Kubernetes-typed status and so stays in the adapter —
// the domain imports no Kubernetes packages.

// HealthLine composes the one-line health summary from ordered segments —
// typically phase, then ready/desired counts, then an optional reason/detail —
// joined with "; ". Empty segments are dropped so an absent detail never leaves
// a dangling separator. It is a glance, not the full status (design note §2,
// §6: the shared composer the RayCluster adapter and the future RayJob/
// RayService extractors all render through).
func HealthLine(segments ...string) string {
	parts := make([]string, 0, len(segments))
	for _, s := range segments {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, "; ")
}

// ConditionReason renders a status condition's reason/message compactly for the
// health line: "reason: message" when both are set, otherwise whichever one is
// present, otherwise empty. The adapter extracts these strings from the typed
// condition and passes them in, keeping this kind-agnostic and I/O-free.
func ConditionReason(reason, message string) string {
	switch {
	case reason != "" && message != "":
		return reason + ": " + message
	case message != "":
		return message
	default:
		return reason
	}
}
