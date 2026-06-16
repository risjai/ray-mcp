package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// envMap returns a getenv-style func backed by a map. A missing key yields the
// empty string, matching os.Getenv semantics, while LookupEnv-style presence is
// modelled by membership in the map (the empty string IS a present value).
func envMap(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	// No SA namespace file -> namespace falls back to "default".
	cfg, err := load(t, nil, nil, "/nonexistent/sa/namespace")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Transport", cfg.Transport, "stdio"},
		{"Context", cfg.Context, ""},
		{"Kubeconfig", cfg.Kubeconfig, ""},
		{"DefaultNamespace", cfg.DefaultNamespace, "default"},
		{"AllowAllNamespaces", cfg.AllowAllNamespaces, false},
		{"RayAccess", cfg.RayAccess, "auto"},
		{"AllowMutations", cfg.AllowMutations, false},
		{"AllowDestructive", cfg.AllowDestructive, false},
		{"AllowRawSpec", cfg.AllowRawSpec, true},
		{"LogLevel", cfg.LogLevel, "info"},
		{"HTTPAddr", cfg.HTTPAddr, "127.0.0.1:8765"},
		{"AuthMode", cfg.AuthMode, "static"},
		{"AuthToken", cfg.AuthToken, ""},
		{"RayDashboardAuth", cfg.RayDashboardAuth, ""},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("default %s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadEnvOverridesDefault(t *testing.T) {
	env := map[string]string{
		"RAY_MCP_TRANSPORT":         "http",
		"RAY_MCP_CONTEXT":           "prod",
		"KUBECONFIG":                "/home/u/.kube/config",
		"RAY_MCP_NAMESPACE":         "team-a",
		"RAY_MCP_ALL_NS":            "true",
		"RAY_MCP_RAY_ACCESS":        "direct",
		"RAY_MCP_ALLOW_MUTATIONS":   "true",
		"RAY_MCP_ALLOW_DESTRUCTIVE": "true",
		"RAY_MCP_ALLOW_RAW_SPEC":    "false",
		"RAY_MCP_LOG_LEVEL":         "debug",
		"RAY_MCP_HTTP_ADDR":         "127.0.0.1:9000",
		"RAY_MCP_AUTH_MODE":         "tokenreview",
		"RAY_MCP_AUTH_TOKEN":        "secret-tok",
		"RAY_MCP_RAY_DASH_AUTH":     "dash-tok",
	}
	cfg, err := load(t, nil, env, "/nonexistent/sa/namespace")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"Transport", cfg.Transport, "http"},
		{"Context", cfg.Context, "prod"},
		{"Kubeconfig", cfg.Kubeconfig, "/home/u/.kube/config"},
		{"DefaultNamespace", cfg.DefaultNamespace, "team-a"},
		{"AllowAllNamespaces", cfg.AllowAllNamespaces, true},
		{"RayAccess", cfg.RayAccess, "direct"},
		{"AllowMutations", cfg.AllowMutations, true},
		{"AllowDestructive", cfg.AllowDestructive, true},
		{"AllowRawSpec", cfg.AllowRawSpec, false},
		{"LogLevel", cfg.LogLevel, "debug"},
		{"HTTPAddr", cfg.HTTPAddr, "127.0.0.1:9000"},
		{"AuthMode", cfg.AuthMode, "tokenreview"},
		{"AuthToken", cfg.AuthToken, "secret-tok"},
		{"RayDashboardAuth", cfg.RayDashboardAuth, "dash-tok"},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("env %s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestLoadFlagOverridesEnv(t *testing.T) {
	// Both env and flag set -> flag wins (the key precedence proof).
	env := map[string]string{
		"RAY_MCP_TRANSPORT":       "stdio",
		"RAY_MCP_NAMESPACE":       "from-env",
		"RAY_MCP_LOG_LEVEL":       "warn",
		"RAY_MCP_ALLOW_RAW_SPEC":  "true",
		"RAY_MCP_ALLOW_MUTATIONS": "false",
	}
	args := []string{
		"--transport", "http",
		"--default-namespace", "from-flag",
		"--log-level", "error",
		"--allow-raw-spec=false",
		"--allow-mutations=true",
		"--http-addr", "127.0.0.1:9999",
	}
	cfg, err := load(t, args, env, "/nonexistent/sa/namespace")
	if err != nil {
		t.Fatalf("Load() error = %v, want nil", err)
	}

	if cfg.Transport != "http" {
		t.Errorf("Transport = %q, want %q (flag wins)", cfg.Transport, "http")
	}
	if cfg.DefaultNamespace != "from-flag" {
		t.Errorf("DefaultNamespace = %q, want %q (flag wins)", cfg.DefaultNamespace, "from-flag")
	}
	if cfg.LogLevel != "error" {
		t.Errorf("LogLevel = %q, want %q (flag wins)", cfg.LogLevel, "error")
	}
	if cfg.AllowRawSpec != false {
		t.Errorf("AllowRawSpec = %v, want false (flag wins)", cfg.AllowRawSpec)
	}
	if cfg.AllowMutations != true {
		t.Errorf("AllowMutations = %v, want true (flag wins)", cfg.AllowMutations)
	}
}

func TestLoadKubeconfigEnvName(t *testing.T) {
	// The odd-one-out: env var is KUBECONFIG, not RAY_MCP_KUBECONFIG.
	env := map[string]string{"KUBECONFIG": "/etc/kube/cfg"}
	cfg, err := load(t, nil, env, "/nonexistent/sa/namespace")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Kubeconfig != "/etc/kube/cfg" {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, "/etc/kube/cfg")
	}

	// A RAY_MCP_KUBECONFIG var must NOT be honoured.
	env2 := map[string]string{"RAY_MCP_KUBECONFIG": "/wrong"}
	cfg2, err := load(t, nil, env2, "/nonexistent/sa/namespace")
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg2.Kubeconfig != "" {
		t.Errorf("Kubeconfig = %q, want empty (RAY_MCP_KUBECONFIG is not a recognised var)", cfg2.Kubeconfig)
	}
}

func TestLoadBindAuthInvariant(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr bool
	}{
		{
			name:    "non-loopback http no token refuses boot",
			args:    []string{"--transport", "http", "--http-addr", "0.0.0.0:8765"},
			wantErr: true,
		},
		{
			name:    "non-loopback http static token ok",
			args:    []string{"--transport", "http", "--http-addr", "0.0.0.0:8765", "--auth-token", "tok"},
			wantErr: false,
		},
		{
			name:    "non-loopback http tokenreview ok",
			args:    []string{"--transport", "http", "--http-addr", "10.0.0.5:8765", "--auth-mode", "tokenreview"},
			wantErr: false,
		},
		{
			name:    "non-loopback http static mode empty token refuses",
			args:    []string{"--transport", "http", "--http-addr", "0.0.0.0:8765", "--auth-mode", "static"},
			wantErr: true,
		},
		{
			name:    "loopback http no token ok (127.0.0.1)",
			args:    []string{"--transport", "http", "--http-addr", "127.0.0.1:8765"},
			wantErr: false,
		},
		{
			name:    "loopback http no token ok (::1)",
			args:    []string{"--transport", "http", "--http-addr", "[::1]:8765"},
			wantErr: false,
		},
		{
			name:    "loopback http no token ok (localhost)",
			args:    []string{"--transport", "http", "--http-addr", "localhost:8765"},
			wantErr: false,
		},
		{
			name:    "empty host is non-loopback, refuses",
			args:    []string{"--transport", "http", "--http-addr", ":8765"},
			wantErr: true,
		},
		{
			name:    "stdio transport no token ok even on non-loopback addr",
			args:    []string{"--transport", "stdio", "--http-addr", "0.0.0.0:8765"},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(t, tt.args, nil, "/nonexistent/sa/namespace")
			if tt.wantErr && err == nil {
				t.Fatalf("Load() error = nil, want non-nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("Load() error = %v, want nil", err)
			}
		})
	}
}

func TestLoadBindAuthErrorMentionsAddress(t *testing.T) {
	_, err := load(t, []string{"--transport", "http", "--http-addr", "0.0.0.0:8765"}, nil, "/nonexistent/sa/namespace")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "0.0.0.0:8765") {
		t.Errorf("error %q should name the bind address", err.Error())
	}
}

func TestLoadDefaultNamespaceFallback(t *testing.T) {
	// Write an SA namespace file we can point at.
	dir := t.TempDir()
	saFile := filepath.Join(dir, "namespace")
	if err := os.WriteFile(saFile, []byte("pod-ns\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		args   []string
		env    map[string]string
		saPath string
		want   string
	}{
		{
			name:   "flag beats everything",
			args:   []string{"--default-namespace", "flag-ns"},
			env:    map[string]string{"RAY_MCP_NAMESPACE": "env-ns"},
			saPath: saFile,
			want:   "flag-ns",
		},
		{
			name:   "env beats SA file",
			env:    map[string]string{"RAY_MCP_NAMESPACE": "env-ns"},
			saPath: saFile,
			want:   "env-ns",
		},
		{
			name:   "SA file contents beat default",
			saPath: saFile,
			want:   "pod-ns",
		},
		{
			name:   "default when SA file absent",
			saPath: "/nonexistent/sa/namespace",
			want:   "default",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := load(t, tt.args, tt.env, tt.saPath)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.DefaultNamespace != tt.want {
				t.Errorf("DefaultNamespace = %q, want %q", cfg.DefaultNamespace, tt.want)
			}
		})
	}
}

func TestLoadEnumRejection(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"bad transport", []string{"--transport", "grpc"}},
		{"bad ray-access", []string{"--ray-access", "magic"}},
		{"bad auth-mode", []string{"--auth-mode", "oauth"}},
		{"bad log-level", []string{"--log-level", "trace"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(t, tt.args, nil, "/nonexistent/sa/namespace")
			if err == nil {
				t.Fatalf("Load(%v) error = nil, want non-nil enum rejection", tt.args)
			}
		})
	}
}

func TestLoadEnumRejectionFromEnv(t *testing.T) {
	// Enum validation must apply to env-sourced values too.
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"bad transport env", map[string]string{"RAY_MCP_TRANSPORT": "grpc"}},
		{"bad ray-access env", map[string]string{"RAY_MCP_RAY_ACCESS": "magic"}},
		{"bad auth-mode env", map[string]string{"RAY_MCP_AUTH_MODE": "oauth"}},
		{"bad log-level env", map[string]string{"RAY_MCP_LOG_LEVEL": "trace"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := load(t, nil, tt.env, "/nonexistent/sa/namespace")
			if err == nil {
				t.Fatalf("Load(env=%v) error = nil, want non-nil enum rejection", tt.env)
			}
		})
	}
}

func TestLoadBadBoolEnv(t *testing.T) {
	_, err := load(t, nil, map[string]string{"RAY_MCP_ALLOW_MUTATIONS": "yes"}, "/nonexistent/sa/namespace")
	if err == nil {
		t.Fatal("Load() error = nil, want non-nil for unparsable bool env")
	}
}

// load is a test helper that invokes Load with the injected SA namespace path
// and a map-backed getenv. A nil env yields an always-absent getenv.
func load(t *testing.T, args []string, env map[string]string, saPath string) (*Config, error) {
	t.Helper()
	getenv := envMap(env)
	return Load(args, WithGetenv(getenv), WithSANamespaceFile(saPath))
}
