package kuberay

import (
	"context"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/risjai/ray-mcp/internal/domain"
)

// clusterLabel is the KubeRay-stamped label every pod of a cluster carries
// (head + all worker groups). It is exact and non-overridable in v1.6.1, so
// selecting on it alone returns the whole cluster's pod set without needing the
// node-type/group labels.
const clusterLabel = "ray.io/cluster"

// defaultEventLimit is the per-call cap applied when the caller passes limit<=0.
// Events are chatty (a single misscheduled pod emits FailedScheduling on a
// loop), so the default is deliberately small — a relevant slice, never the raw
// firehose (spec §10).
const defaultEventLimit = 25

// eventListCap bounds the namespace EventList read before client-side filtering.
// We list the namespace's core/v1 events once (one round-trip) and filter to the
// cluster + its pods, rather than a field-selected GET per object; this cap keeps
// that single list from itself becoming unbounded on a busy namespace.
const eventListCap = 500

// maxMessageLen truncates each event message so one pathological message (a long
// scheduler dump, a stack trace) cannot blow the token budget. Trimmed messages
// get an ellipsis marker.
const maxMessageLen = 250

// Events returns recent, bounded k8s events for a RayCluster: the operator
// events recorded against the RayCluster object itself merged with the
// scheduler/kubelet events of its pods, time-sorted recent-first with Warnings
// prioritized (spec §10 — a relevant slice, never the raw firehose).
//
// Only KindRayCluster is supported today (the only event tool is
// ray_cluster_events); other kinds return an empty slice rather than an error so
// the contract stays additive.
//
// Approach: resolve the relevant involvedObject names first (the cluster name,
// given, plus its pod names from a label-selected PodList), then list the
// namespace's core/v1 events once (capped) and filter client-side to those names.
// Events cannot be label-selected, so name resolution is the only way to scope
// them; one capped list + a client-side filter is fewer round-trips than a
// field-selected GET per object.
func (c *Client) Events(ctx context.Context, kind domain.Kind, namespace, name string, limit int) ([]domain.Event, error) {
	if kind != domain.KindRayCluster {
		return nil, nil
	}

	k8s, err := c.ensureClient()
	if err != nil {
		return nil, err
	}

	if limit <= 0 {
		limit = defaultEventLimit
	}

	// The cluster object's own name is always a target; pod names join it.
	targets := map[string]struct{}{name: {}}

	var pods corev1.PodList
	if err := k8s.List(ctx, &pods,
		client.InNamespace(namespace),
		client.MatchingLabels{clusterLabel: name},
	); err != nil {
		return nil, mapK8sError(err, "list", domain.KindRayCluster, namespace, name)
	}
	for i := range pods.Items {
		targets[pods.Items[i].Name] = struct{}{}
	}

	var events corev1.EventList
	if err := k8s.List(ctx, &events,
		client.InNamespace(namespace),
		client.Limit(eventListCap),
	); err != nil {
		return nil, mapK8sError(err, "list events for", domain.KindRayCluster, namespace, name)
	}

	relevant := make([]domain.Event, 0, len(events.Items))
	for i := range events.Items {
		ev := &events.Items[i]
		if _, ok := targets[ev.InvolvedObject.Name]; !ok {
			continue
		}
		relevant = append(relevant, toEvent(ev))
	}

	return boundEvents(relevant, limit), nil
}

// toEvent maps a core/v1 Event to the bounded domain DTO: the resolved last-seen
// timestamp, the resolved occurrence count, the verbatim type/reason, and the
// truncated message.
func toEvent(ev *corev1.Event) domain.Event {
	return domain.Event{
		Type:     ev.Type,
		Reason:   ev.Reason,
		Message:  truncateMessage(ev.Message),
		Count:    eventCount(ev),
		LastSeen: eventLastSeen(ev),
	}
}

// eventCount resolves the occurrence count, preferring the series count (set on
// aggregated events) over the legacy .count.
func eventCount(ev *corev1.Event) int32 {
	if ev.Series != nil && ev.Series.Count > 0 {
		return ev.Series.Count
	}
	return ev.Count
}

// eventLastSeen resolves the most-recent-occurrence timestamp with the documented
// fallback ladder: lastTimestamp → series.lastObservedTime → eventTime →
// firstTimestamp. The legacy core/v1 path populates lastTimestamp; the newer
// aggregated path populates series.lastObservedTime / eventTime instead, so a
// single field is not enough.
func eventLastSeen(ev *corev1.Event) time.Time {
	if !ev.LastTimestamp.IsZero() {
		return ev.LastTimestamp.Time
	}
	if ev.Series != nil && !ev.Series.LastObservedTime.IsZero() {
		return ev.Series.LastObservedTime.Time
	}
	if !ev.EventTime.IsZero() {
		return ev.EventTime.Time
	}
	return ev.FirstTimestamp.Time
}

// truncateMessage caps an event message at maxMessageLen, appending an ellipsis
// marker so the trim is visible and one huge message cannot dominate the budget.
func truncateMessage(msg string) string {
	if len(msg) <= maxMessageLen {
		return msg
	}
	return msg[:maxMessageLen] + "…"
}

// boundEvents applies the token-economy bounding: sort Warnings-first then
// recency (Warnings are the actionable signal — FailedScheduling, OOMKilled,
// ErrImagePull — so they survive the trim while chatty Normals are dropped
// first), then take the first `limit`.
func boundEvents(events []domain.Event, limit int) []domain.Event {
	sort.SliceStable(events, func(i, j int) bool {
		wi, wj := isWarning(events[i]), isWarning(events[j])
		if wi != wj {
			return wi // Warning before Normal.
		}
		return events[i].LastSeen.After(events[j].LastSeen) // then most recent first.
	})

	if len(events) > limit {
		events = events[:limit]
	}
	return events
}

// isWarning reports whether an event is a Warning (the actionable tier).
func isWarning(ev domain.Event) bool {
	return ev.Type == corev1.EventTypeWarning
}
