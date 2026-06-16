// Package domain is the service layer and imports no Kubernetes or HTTP
// packages. It holds the cluster/job/service services, the safety guards, the
// unified apply pipeline (curated params + rawSpec merge, dry-run, diff), and
// status distillation. It depends only on Go interfaces (ports), so it is
// unit-testable with fakes. See tasks/plan.md.
package domain
