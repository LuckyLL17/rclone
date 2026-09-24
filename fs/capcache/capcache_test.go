package capcache_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/capcache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/rclone/rclone/backend/local"
	"github.com/rclone/rclone/fstest/mockfs"
)

func init() {
	mockfs.Register()
}

// setup points the cache at a temp directory and returns a configured context.
func setup(t *testing.T, enabled bool, ttl time.Duration) (context.Context, string) {
	t.Helper()
	dir := t.TempDir()
	prevCacheDir := capcache.CacheDir
	capcache.CacheDir = func() string { return dir }
	ctx, ci := fs.AddConfig(context.Background())
	ci.CapabilityCache = enabled
	ci.CapabilityCacheTTL = fs.Duration(ttl)
	t.Cleanup(func() {
		capcache.CacheDir = prevCacheDir
	})
	return ctx, dir
}

// newLocalFs creates a local backend in a temp directory.
func newLocalFs(t *testing.T, ctx context.Context) fs.Fs {
	t.Helper()
	dir := t.TempDir()
	f, err := fs.NewFs(ctx, dir)
	require.NoError(t, err)
	return f
}

// cacheFilePath returns where the cache file should be.
func cacheFilePath(root string) string {
	return filepath.Join(root, "capcache", "capcache.json")
}

func TestGetDisabled(t *testing.T) {
	ctx, root := setup(t, false, time.Hour)
	f := newLocalFs(t, ctx)

	p, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.Equal(t, "local", p.Key.Backend)
	assert.NotNil(t, p.Features)
	assert.True(t, p.ExpiresAt.IsZero())

	_, statErr := os.Stat(cacheFilePath(root))
	assert.True(t, os.IsNotExist(statErr))
	assert.Empty(t, capcache.List())

	deleted, err := capcache.Invalidate(ctx, f)
	require.NoError(t, err)
	assert.False(t, deleted)
	_, statErr = os.Stat(cacheFilePath(root))
	assert.True(t, os.IsNotExist(statErr))
}

func TestGetEnabled(t *testing.T) {
	ctx, root := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	p1, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.False(t, p1.Expired())
	assert.False(t, p1.ExpiresAt.IsZero())
	assert.FileExists(t, cacheFilePath(root))

	require.Len(t, capcache.List(), 1)

	p2, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.Same(t, p1, p2)
}

func TestTTLExpiry(t *testing.T) {
	ctx, _ := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	p1, err := capcache.Get(ctx, f)
	require.NoError(t, err)

	require.Len(t, capcache.List(), 1)
	capcache.List()[0].ExpiresAt = time.Now().Add(-time.Second)
	assert.True(t, p1.Expired())

	p2, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.False(t, p2.Expired())
	assert.NotEqual(t, p1.FetchedAt, p2.FetchedAt)
	require.Len(t, capcache.List(), 1)
}

func TestTTLZeroNeverExpires(t *testing.T) {
	ctx, _ := setup(t, true, 0)
	f := newLocalFs(t, ctx)

	p, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.True(t, p.ExpiresAt.IsZero())
	assert.False(t, p.Expired())

	p2, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.Same(t, p, p2)
}

func TestRefresh(t *testing.T) {
	ctx, _ := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	p1, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	capcache.List()[0].ExpiresAt = time.Now().Add(-time.Second)

	p2, err := capcache.Refresh(ctx, f)
	require.NoError(t, err)
	assert.False(t, p2.Expired())
	assert.NotEqual(t, p1.FetchedAt, p2.FetchedAt)
	assert.Equal(t, p1.Key, p2.Key)
	require.Len(t, capcache.List(), 1)
}

func TestInvalidate(t *testing.T) {
	ctx, _ := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	_, err := capcache.Get(ctx, f)
	require.NoError(t, err)

	deleted, err := capcache.Invalidate(ctx, f)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.Empty(t, capcache.List())

	deleted, err = capcache.Invalidate(ctx, f)
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestInvalidateBackend(t *testing.T) {
	ctx, _ := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)
	fMock, err := mockfs.NewFs(ctx, ":mockfs", "/", nil)
	require.NoError(t, err)

	_, err = capcache.Get(ctx, f)
	require.NoError(t, err)
	_, err = capcache.Get(ctx, fMock)
	require.NoError(t, err)
	require.Len(t, capcache.List(), 2)

	deleted, err := capcache.InvalidateBackend("mockfs")
	require.NoError(t, err)
	assert.Equal(t, 1, deleted)
	require.Len(t, capcache.List(), 1)
	assert.Equal(t, "local", capcache.List()[0].Key.Backend)

	deleted, err = capcache.InvalidateBackend("nothing")
	require.NoError(t, err)
	assert.Equal(t, 0, deleted)
}

func TestClear(t *testing.T) {
	ctx, _ := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	_, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	require.Len(t, capcache.List(), 1)

	require.NoError(t, capcache.Clear())
	assert.Empty(t, capcache.List())
}

func TestFingerprint(t *testing.T) {
	ctx, _ := setup(t, false, 0)
	dir := t.TempDir()

	f1, err := fs.NewFs(ctx, dir)
	require.NoError(t, err)
	p1, err := capcache.Get(ctx, f1)
	require.NoError(t, err)
	p1b, err := capcache.Get(ctx, f1)
	require.NoError(t, err)
	assert.Equal(t, p1.Key, p1b.Key)
	assert.Equal(t, "local", p1.Key.Backend)
	assert.Len(t, p1.Key.Fingerprint, 64)
	assert.Empty(t, p1.Key.AuthContext)

	f2 := newLocalFs(t, ctx)
	p2, err := capcache.Get(ctx, f2)
	require.NoError(t, err)
	assert.NotEqual(t, p1.Key.Fingerprint, p2.Key.Fingerprint)
}

func TestPersistence(t *testing.T) {
	ctx, root := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	p1, err := capcache.Get(ctx, f)
	require.NoError(t, err)
	assert.FileExists(t, cacheFilePath(root))

	data, err := os.ReadFile(cacheFilePath(root))
	require.NoError(t, err)
	var onDisk struct {
		Version int                            `json:"version"`
		Entries map[string]capcache.Profile    `json:"entries"`
	}
	require.NoError(t, json.Unmarshal(data, &onDisk))
	assert.Equal(t, 1, onDisk.Version)
	require.Len(t, onDisk.Entries, 1)
	for _, stored := range onDisk.Entries {
		assert.Equal(t, p1.Key, stored.Key)
		assert.Equal(t, p1.Hashes, stored.Hashes)
	}
}

func TestProbeIgnoresCache(t *testing.T) {
	ctx, _ := setup(t, true, time.Hour)
	f := newLocalFs(t, ctx)

	p1, err := capcache.Get(ctx, f)
	require.NoError(t, err)

	p2, err := capcache.Probe(ctx, f)
	require.NoError(t, err)
	assert.NotSame(t, p1, p2)
	assert.Equal(t, p1.Key, p2.Key)
}

func TestProfileMarshalJSON(t *testing.T) {
	p := capcache.Profile{
		Key:       capcache.Key{Backend: "local", Fingerprint: "abc", AuthContext: ""},
		Features:  map[string]bool{"IsLocal": true},
		Hashes:    []string{"md5"},
		Precision: time.Second,
		FetchedAt: time.Now(),
	}
	data, err := json.Marshal(p)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Equal(t, false, out["stale"])
	assert.Equal(t, "off", out["ttl"])
	assert.Equal(t, "1s", out["precisionString"])

	p.ExpiresAt = time.Now().Add(time.Hour)
	data, err = json.Marshal(p)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(data, &out))
	assert.Contains(t, out["ttl"], "m")
}
