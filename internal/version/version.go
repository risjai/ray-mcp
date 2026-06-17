// Package version holds the build-time identity constants reported by the
// server (e.g. via the ray_capabilities meta tool). They are plain constants
// for now; a release build may override Version via -ldflags later.
package version

const (
	// Version is the ray-mcp server version. It is a build-time constant; a
	// release build may stamp it via -ldflags, but the default lives here and is
	// bumped at each tagged release. v0.1.0 is the read-only preview: the
	// RayCluster read path (list/get/events) + ray_capabilities over stdio.
	Version = "0.1.0"

	// KubeRayTested is the KubeRay version range ray-mcp is compiled and
	// CI-tested against. It is reported verbatim by ray_capabilities — Task 4
	// makes no live query, so this is the only KubeRay-version signal surfaced
	// (served-API-group-version / crdVersion are deferred to later tasks).
	KubeRayTested = "v1.6.1"
)
