// Package kuberay implements the KubeRayPort using the controller-runtime
// client package (uncached, direct-to-API-server) with the KubeRay Go types.
// All mutations go through Server-Side Apply preceded by a dry-run; it also
// reads the installed Ray CRD schema for pruning prediction. See tasks/plan.md.
package kuberay
