// Package rayapi implements the read-only RayAPIPort: a client for the Ray
// dashboard / job-submission REST API (status and logs). By construction it
// exposes no write methods — every mutation goes through the CRD path — so the
// unauthenticated Ray dashboard is never a write vector. See tasks/plan.md.
package rayapi
