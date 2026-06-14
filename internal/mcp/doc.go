// Package mcp is the MCP edge: tool registration, JSON schemas, and the mapping
// between decoded tool arguments and domain DTOs, plus result/error formatting
// (structured + text). It depends on the official modelcontextprotocol/go-sdk
// and isolates that SDK from the domain and adapter layers. See tasks/plan.md.
package mcp
