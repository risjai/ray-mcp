package mcp

import (
	"slices"
	"testing"

	"github.com/risjai/ray-mcp/internal/config"
)

// TestEnabledTiers exercises the pure tier-derivation directly (white-box): read
// is always on, write tracks --allow-mutations, destructive tracks
// --allow-destructive.
func TestEnabledTiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  *config.Config
		want []string
	}{
		{"read only", &config.Config{}, []string{"read"}},
		{"write only", &config.Config{AllowMutations: true}, []string{"read", "write"}},
		{"destructive only", &config.Config{AllowDestructive: true}, []string{"read", "destructive"}},
		{"all", &config.Config{AllowMutations: true, AllowDestructive: true}, []string{"read", "write", "destructive"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := enabledTiers(tt.cfg); !slices.Equal(got, tt.want) {
				t.Errorf("enabledTiers(%+v) = %v, want %v", tt.cfg, got, tt.want)
			}
		})
	}
}
