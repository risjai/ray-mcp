// Package reachability resolves how to reach a Ray head's dashboard endpoint:
// DirectDial in-cluster (via cluster DNS) versus a pooled SPDY PortForward
// out-of-cluster, selected by the --ray-access strategy. Tunnels are pooled per
// (namespace, cluster) with an idle-timeout reaper. See tasks/plan.md.
package reachability
