// Package config holds the server configuration struct and its loading rules:
// flag and environment parsing with flags > environment > defaults precedence,
// plus the static boot invariants that need no cluster (e.g. a non-loopback
// HTTP bind requires an auth token). See tasks/plan.md Task 2.
package config
