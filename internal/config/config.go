package config

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
)

// defaultSANamespaceFile is the in-cluster path to the pod's own namespace,
// projected by the service-account token volume. It is read as the
// default-namespace fallback when no flag or env var is set.
const defaultSANamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

// Config is the resolved server configuration. Every field is populated by
// Load with flags > environment > defaults precedence, and the boot invariants
// (§9) have already been validated by the time a *Config is returned.
type Config struct {
	Transport          string // stdio | http
	Context            string // kubeconfig context; "" = current context
	Kubeconfig         string // kubeconfig path; "" = discovery / in-cluster
	DefaultNamespace   string // namespace used when a tool omits one
	AllowAllNamespaces bool   // permit cluster-wide list
	RayAccess          string // auto | direct | port-forward
	AllowMutations     bool   // register write tools
	AllowDestructive   bool   // register destructive tools
	AllowRawSpec       bool   // expose rawSpec in tool schemas (default true)
	LogLevel           string // debug | info | warn | error
	HTTPAddr           string // host:port for the http transport
	AuthMode           string // static | tokenreview
	AuthToken          string // static bearer token; "" = none
	RayDashboardAuth   string // token/header passed to a dashboard auth proxy; "" = none
}

// option configures Load. It exists so tests can inject the environment and the
// service-account namespace file path without touching the real process or
// filesystem.
type option struct {
	getenv   func(string) (string, bool)
	saNSFile string
}

// Option customises Load.
type Option func(*option)

// WithGetenv injects a LookupEnv-style environment lookup. The bool reports
// whether the variable is present (an empty present value is distinct from
// absent). Defaults to os.LookupEnv.
func WithGetenv(getenv func(string) (string, bool)) Option {
	return func(o *option) { o.getenv = getenv }
}

// WithSANamespaceFile overrides the path read for the in-cluster default
// namespace fallback. Defaults to defaultSANamespaceFile.
func WithSANamespaceFile(path string) Option {
	return func(o *option) { o.saNSFile = path }
}

var (
	transportValues = []string{"stdio", "http"}
	rayAccessValues = []string{"auto", "direct", "port-forward"}
	authModeValues  = []string{"static", "tokenreview"}
	logLevelValues  = []string{"debug", "info", "warn", "error"}
)

// Load resolves configuration from args, the environment, and defaults
// (precedence: flags > environment > defaults) and validates the static boot
// invariants that need no cluster. args is the flag slice (typically
// os.Args[1:]); a *flag.FlagSet is built fresh per call so Load is safe to call
// from tests.
func Load(args []string, opts ...Option) (*Config, error) {
	o := option{getenv: os.LookupEnv, saNSFile: defaultSANamespaceFile}
	for _, opt := range opts {
		opt(&o)
	}

	fs := flag.NewFlagSet("ray-mcp", flag.ContinueOnError)
	// Suppress flag's own usage dump on parse error; Load returns the error.
	fs.SetOutput(io.Discard)

	// Register every flag with its spec default. Resolution of env/defaults
	// happens after Parse via fs.Visit (which only reports set flags), so these
	// defaults are only the floor of the precedence stack.
	var (
		transport          = fs.String("transport", "stdio", "")
		kubeContext        = fs.String("context", "", "")
		kubeconfig         = fs.String("kubeconfig", "", "")
		defaultNamespace   = fs.String("default-namespace", "", "")
		allowAllNamespaces = fs.Bool("allow-all-namespaces", false, "")
		rayAccess          = fs.String("ray-access", "auto", "")
		allowMutations     = fs.Bool("allow-mutations", false, "")
		allowDestructive   = fs.Bool("allow-destructive", false, "")
		allowRawSpec       = fs.Bool("allow-raw-spec", true, "")
		logLevel           = fs.String("log-level", "info", "")
		httpAddr           = fs.String("http-addr", "127.0.0.1:8765", "")
		authMode           = fs.String("auth-mode", "static", "")
		authToken          = fs.String("auth-token", "", "")
		rayDashboardAuth   = fs.String("ray-dashboard-auth", "", "")
	)

	if err := fs.Parse(args); err != nil {
		return nil, fmt.Errorf("parse flags: %w", err)
	}

	set := setFlags(fs)

	cfg := &Config{
		Transport:        resolveStr(set, "transport", *transport, "RAY_MCP_TRANSPORT", o.getenv),
		Context:          resolveStr(set, "context", *kubeContext, "RAY_MCP_CONTEXT", o.getenv),
		Kubeconfig:       resolveStr(set, "kubeconfig", *kubeconfig, "KUBECONFIG", o.getenv),
		RayAccess:        resolveStr(set, "ray-access", *rayAccess, "RAY_MCP_RAY_ACCESS", o.getenv),
		LogLevel:         resolveStr(set, "log-level", *logLevel, "RAY_MCP_LOG_LEVEL", o.getenv),
		HTTPAddr:         resolveStr(set, "http-addr", *httpAddr, "RAY_MCP_HTTP_ADDR", o.getenv),
		AuthMode:         resolveStr(set, "auth-mode", *authMode, "RAY_MCP_AUTH_MODE", o.getenv),
		AuthToken:        resolveStr(set, "auth-token", *authToken, "RAY_MCP_AUTH_TOKEN", o.getenv),
		RayDashboardAuth: resolveStr(set, "ray-dashboard-auth", *rayDashboardAuth, "RAY_MCP_RAY_DASH_AUTH", o.getenv),
	}

	var err error
	if cfg.AllowAllNamespaces, err = resolveBool(set, "allow-all-namespaces", *allowAllNamespaces, "RAY_MCP_ALL_NS", o.getenv); err != nil {
		return nil, err
	}
	if cfg.AllowMutations, err = resolveBool(set, "allow-mutations", *allowMutations, "RAY_MCP_ALLOW_MUTATIONS", o.getenv); err != nil {
		return nil, err
	}
	if cfg.AllowDestructive, err = resolveBool(set, "allow-destructive", *allowDestructive, "RAY_MCP_ALLOW_DESTRUCTIVE", o.getenv); err != nil {
		return nil, err
	}
	if cfg.AllowRawSpec, err = resolveBool(set, "allow-raw-spec", *allowRawSpec, "RAY_MCP_ALLOW_RAW_SPEC", o.getenv); err != nil {
		return nil, err
	}

	cfg.DefaultNamespace = resolveNamespace(set, *defaultNamespace, o)

	if err := validate(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// setFlags returns the set of flag names that were explicitly present on the
// command line. fs.Visit only iterates flags that were set, which is how we
// distinguish "flag given" from "flag left at default".
func setFlags(fs *flag.FlagSet) map[string]bool {
	set := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	return set
}

// resolveStr applies flags > env > default for a string field. flagVal already
// equals the spec default when the flag was not set, so it doubles as the
// default tier.
func resolveStr(set map[string]bool, name, flagVal, envKey string, getenv func(string) (string, bool)) string {
	if set[name] {
		return flagVal
	}
	if v, ok := getenv(envKey); ok {
		return v
	}
	return flagVal
}

// resolveBool applies flags > env > default for a bool field, parsing the env
// value with the same rules as flag (strconv.ParseBool). An unparsable env
// value is an error rather than a silent fallback.
func resolveBool(set map[string]bool, name string, flagVal bool, envKey string, getenv func(string) (string, bool)) (bool, error) {
	if set[name] {
		return flagVal, nil
	}
	if v, ok := getenv(envKey); ok {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, fmt.Errorf("invalid boolean for %s=%q: %w", envKey, v, err)
		}
		return b, nil
	}
	return flagVal, nil
}

// resolveNamespace implements the §9 fallback ordering:
// flag > env (RAY_MCP_NAMESPACE) > SA-namespace-file contents > "default".
func resolveNamespace(set map[string]bool, flagVal string, o option) string {
	if set["default-namespace"] {
		return flagVal
	}
	if v, ok := o.getenv("RAY_MCP_NAMESPACE"); ok {
		return v
	}
	if data, err := os.ReadFile(o.saNSFile); err == nil {
		if ns := strings.TrimSpace(string(data)); ns != "" {
			return ns
		}
	}
	return "default"
}

// validate checks the static boot invariants (§9) that need no cluster: enum
// membership and the non-loopback bind/auth invariant (Q8).
func validate(cfg *Config) error {
	if !oneOf(cfg.Transport, transportValues) {
		return fmt.Errorf("invalid --transport %q: must be one of %s", cfg.Transport, strings.Join(transportValues, "|"))
	}
	if !oneOf(cfg.RayAccess, rayAccessValues) {
		return fmt.Errorf("invalid --ray-access %q: must be one of %s", cfg.RayAccess, strings.Join(rayAccessValues, "|"))
	}
	if !oneOf(cfg.AuthMode, authModeValues) {
		return fmt.Errorf("invalid --auth-mode %q: must be one of %s", cfg.AuthMode, strings.Join(authModeValues, "|"))
	}
	if !oneOf(cfg.LogLevel, logLevelValues) {
		return fmt.Errorf("invalid --log-level %q: must be one of %s", cfg.LogLevel, strings.Join(logLevelValues, "|"))
	}

	// Bind/auth invariant: only the http transport binds a listener.
	if cfg.Transport == "http" && !isLoopback(cfg.HTTPAddr) {
		hasToken := (cfg.AuthMode == "static" && cfg.AuthToken != "") || cfg.AuthMode == "tokenreview"
		if !hasToken {
			return fmt.Errorf(
				"refusing to boot: --http-addr %q binds a non-loopback address without auth; "+
					"set --auth-token (static mode) or --auth-mode tokenreview (no --insecure escape hatch)",
				cfg.HTTPAddr,
			)
		}
	}
	return nil
}

// isLoopback reports whether host:port binds a loopback address. The literal
// host "localhost" is treated as loopback; an empty, 0.0.0.0, or :: host is
// non-loopback (it binds all interfaces). A non-IP, non-localhost host is
// treated conservatively as non-loopback.
func isLoopback(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// No port / unparsable: treat the whole string as the host.
		host = addr
	}
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}

func oneOf(v string, allowed []string) bool {
	for _, a := range allowed {
		if v == a {
			return true
		}
	}
	return false
}
