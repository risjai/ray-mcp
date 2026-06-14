// Package observability holds structured logging and the mutation audit log.
// In stdio transport, stdout is the MCP JSON-RPC wire, so all logging and audit
// output must go to stderr or a file — never stdout. See tasks/plan.md.
package observability
