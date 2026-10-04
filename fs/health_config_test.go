package fs_test

import (
	"context"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
)

func optionDefault(t *testing.T, name string) any {
	t.Helper()
	for i := range fs.ConfigOptionsInfo {
		opt := &fs.ConfigOptionsInfo[i]
		if opt.Name == name {
			return opt.Default
		}
	}
	t.Fatalf("option %q not found", name)
	return nil
}

func TestHealthOptionDefaults(t *testing.T) {
	assert.Equal(t, false, optionDefault(t, "health_enable"))
	assert.Equal(t, 5, optionDefault(t, "health_failures"))
	assert.Equal(t, 30*time.Second, optionDefault(t, "health_cooldown"))
	assert.Equal(t, 1, optionDefault(t, "health_probe_success"))
}

func TestHealthFlagsRegistered(t *testing.T) {
	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.AddFlagsFromOptions(flagSet, "", fs.ConfigOptionsInfo)
	for _, tc := range []struct {
		name    string
		def     string
		typ     string
	}{
		{"health-enable", "false", "bool"},
		{"health-failures", "5", "int"},
		{"health-cooldown", "30s", "Duration"},
		{"health-probe-success", "1", "int"},
	} {
		flag := flagSet.Lookup(tc.name)
		require.NotNil(t, flag, "flag %s", tc.name)
		assert.Equal(t, tc.def, flag.DefValue, "flag %s", tc.name)
		assert.Equal(t, tc.typ, flag.Value.Type(), "flag %s", tc.name)
	}
}

func TestHealthReloadValidation(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.HealthFailures = 0
	require.ErrorContains(t, ci.Reload(ctx), "--health-failures")

	ci.HealthFailures = 5
	ci.HealthProbeSuccess = 0
	require.ErrorContains(t, ci.Reload(ctx), "--health-probe-success")

	ci.HealthProbeSuccess = 1
	ci.HealthCooldown = -1
	require.ErrorContains(t, ci.Reload(ctx), "--health-cooldown")

	ci.HealthCooldown = 0
	require.NoError(t, ci.Reload(ctx))
}
