// Command ray-mcp is an MCP server for managing Ray on Kubernetes via KubeRay.
//
// It exposes tools to manage the lifecycle of RayCluster, RayJob, and
// RayService resources through KubeRay CRDs (the guarded write path) and
// reaches Ray's dashboard/job API read-only for the runtime detail the CRDs do
// not expose (the cross-plane "wedge").
//
// This file currently holds only the entrypoint skeleton; flag parsing,
// wiring, and transport selection arrive in later tasks (see tasks/plan.md).
package main

func main() {
	// Wiring is implemented in Task 4 (walking skeleton). Intentionally empty.
}
