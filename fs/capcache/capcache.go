// Package capcache implements an optional cache of remote capability profiles.
//
// When enabled via --capability-cache, a snapshot of the capabilities of
// each remote (feature flags, supported hashes, mime types, server-side
// copy and listing) is stored under the cache directory keyed by backend
// type, a fingerprint of the resolved configuration and the authenticated
// user context. Copy, sync, check and list operations share the cached
// profile; it expires after the TTL, can be refreshed explicitly and is
// invalidated when the configuration changes.
//
// When the cache is disabled capabilities are probed on demand and
// nothing is stored, which is the default behaviour.
package capcache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/configmap"
	"golang.org/x/sync/singleflight"
)

const (
	cacheVersion = 1
	cacheSubdir  = "capcache"
	cacheFile    = "capcache.json"
	keySeparator = "|"
)

// Key identifies a capability profile.
type Key struct {
	Backend     string `json:"backend"`     // registered backend type, e.g. "s3"
	Fingerprint string `json:"fingerprint"` // fingerprint of the resolved configuration
	AuthContext string `json:"authContext"` // fingerprint of the authenticated user context, "" if not asserted
}

// String renders the key as its on-disk map key.
func (k Key) String() string {
	return k.Backend + keySeparator + k.Fingerprint + keySeparator + k.AuthContext
}

// fingerprintPrefix is the on-disk key prefix covering all auth contexts.
func (k Key) fingerprintPrefix() string {
	return k.Backend + keySeparator + k.Fingerprint + keySeparator
}

// Profile is a cached snapshot of the capabilities of a remote.
type Profile struct {
	Key       Key             `json:"key"`
	Name      string          `json:"name"`  // remote name as created
	Root      string          `json:"root"`  // remote root as created
	Features  map[string]bool `json:"features"`
	Hashes    []string        `json:"hashes"`
	Precision time.Duration   `json:"precision"`
	FetchedAt time.Time       `json:"fetchedAt"`
	ExpiresAt time.Time       `json:"expiresAt"`
}

// MarshalJSON adds derived fields for callers judging freshness.
func (p Profile) MarshalJSON() ([]byte, error) {
	type Alias Profile
	ttl := "off"
	if !p.ExpiresAt.IsZero() {
		ttl = fs.Duration(time.Until(p.ExpiresAt)).String()
	}
	return json.Marshal(struct {
		Alias
		Stale           bool   `json:"stale"`
		TTL             string `json:"ttl"`
		PrecisionString string `json:"precisionString"`
	}{
		Alias:           Alias(p),
		Stale:           p.Expired(),
		TTL:             ttl,
		PrecisionString: fs.Duration(p.Precision).String(),
	})
}

// Expired returns true if the profile is past its TTL.
//
// A zero ExpiresAt means the profile does not expire on time.
func (p *Profile) Expired() bool {
	return !p.ExpiresAt.IsZero() && time.Now().After(p.ExpiresAt)
}

// Enabled returns true if the capability cache is turned on.
func Enabled(ctx context.Context) bool {
	return fs.GetConfig(ctx).CapabilityCache
}

// CacheDir returns the root cache directory.
//
// This is set by fs/config to avoid a circular dependency; if unset
// it falls back to the operating system temporary directory.
var CacheDir func() string

func getCacheDir() string {
	if CacheDir != nil {
		return CacheDir()
	}
	return filepath.Join(os.TempDir(), "rclone")
}

// store is the on-disk format of the cache.
type store struct {
	Version int                 `json:"version"`
	Entries map[string]*Profile `json:"entries"`
}

var (
	storeMu   sync.Mutex
	disk      *store
	loadedDir string
)

// filePath returns the location of the cache file.
func filePath() string {
	return filepath.Join(getCacheDir(), cacheSubdir, cacheFile)
}

// getStore returns the cache, loading it from the cache directory when needed.
//
// Changing the cache directory (or calling it for the first time) loads
// the cache file afresh, which also serves as the new-process simulation
// point for tests.
func getStore() *store {
	dir := getCacheDir()
	if disk != nil && loadedDir == dir {
		return disk
	}
	loadedDir = dir
	disk = &store{Version: cacheVersion, Entries: map[string]*Profile{}}
	data, err := os.ReadFile(filePath())
	if err != nil {
		return disk
	}
	if err := json.Unmarshal(data, disk); err != nil {
		fs.Logf(nil, "Capability cache: discarding unreadable cache %q: %v", filePath(), err)
		disk = &store{Version: cacheVersion, Entries: map[string]*Profile{}}
	}
	return disk
}

// save writes the store atomically, called with storeMu held.
func (s *store) save() error {
	data, err := json.MarshalIndent(s, "", "\t")
	if err != nil {
		return fmt.Errorf("capability cache encode failed: %w", err)
	}
	path := filePath()
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("capability cache mkdir failed: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".capcache*.tmp")
	if err != nil {
		return fmt.Errorf("capability cache temp file failed: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("capability cache write failed: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("capability cache close failed: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("capability cache rename failed: %w", err)
	}
	return nil
}

// lookup returns a fresh stored profile for k.
//
// When exact is false it matches the backend/config fingerprint
// prefix: if a single auth context is stored it is returned without
// probing the authenticated user; UserInfo is only needed to
// disambiguate multiple contexts. With exact set the full key is
// matched. Expired entries are pruned.
func lookup(k Key, exact bool) (p *Profile) {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	prefix := k.fingerprintPrefix()
	var matches []*Profile
	for key, entry := range disk.Entries {
		if exact {
			if key != k.String() {
				continue
			}
		} else if !startsWith(key, prefix) {
			continue
		}
		if entry.Expired() {
			delete(disk.Entries, key)
			continue
		}
		matches = append(matches, entry)
	}
	if len(matches) == 1 {
		p = matches[0]
	}
	return p
}

// pruneExpired removes expired entries for the fingerprint prefix and rewrites the store when changed.
func pruneExpired(k Key) {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	pruned := false
	prefix := k.fingerprintPrefix()
	for key, entry := range disk.Entries {
		if startsWith(key, prefix) && entry.Expired() {
			delete(disk.Entries, key)
			pruned = true
		}
	}
	if pruned {
		if err := disk.save(); err != nil {
			fs.Logf(nil, "Capability cache: failed to rewrite cache: %v", err)
		}
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// put stores a profile and writes the cache.
func put(p *Profile) error {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	disk.Entries[p.Key.String()] = p
	return disk.save()
}

// remove deletes the exact entry for k.
func remove(k Key) (deleted bool, err error) {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	if _, ok := disk.Entries[k.String()]; ok {
		delete(disk.Entries, k.String())
		deleted = true
	}
	if deleted {
		err = disk.save()
	}
	return deleted, err
}

// deleteBackend removes every entry for the backend type.
func deleteBackend(backend string) (deleted int, err error) {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	prefix := backend + keySeparator
	for key := range disk.Entries {
		if startsWith(key, prefix) {
			delete(disk.Entries, key)
			deleted++
		}
	}
	if deleted > 0 {
		err = disk.save()
	}
	return deleted, err
}

// clear removes every entry.
func clear() error {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	disk.Entries = map[string]*Profile{}
	return disk.save()
}

// all returns stored profiles sorted by key, pruning expired entries.
func all() []*Profile {
	storeMu.Lock()
	defer storeMu.Unlock()
	disk = getStore()
	pruned := false
	for key, entry := range disk.Entries {
		if entry.Expired() {
			delete(disk.Entries, key)
			pruned = true
		}
	}
	if pruned {
		if err := disk.save(); err != nil {
			fs.Logf(nil, "Capability cache: failed to rewrite cache: %v", err)
		}
	}
	out := make([]*Profile, 0, len(disk.Entries))
	for _, p := range disk.Entries {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.String() < out[j].Key.String() })
	return out
}

// localKey builds the key without probing the authenticated user context.
func localKey(f fs.Fs) (k Key, err error) {
	fsInfo, configName, _, connConfig, err := fs.ParseRemote(fs.ConfigStringFull(f))
	if err != nil {
		return Key{}, err
	}
	return Key{
		Backend:     fsInfo.Name,
		Fingerprint: fingerprint(fsInfo, configName, f.Root(), connConfig),
	}, nil
}

// withAuth fills in the authenticated user context of k.
func (k *Key) withAuth(ctx context.Context, f fs.Fs) {
	k.AuthContext = authContext(ctx, f.Features())
}

// fingerprint hashes the resolved configuration of a backend.
//
// Password and sensitive option values are replaced by a presence
// marker so refreshed tokens do not change the fingerprint, while
// adding or removing the option still does.
func fingerprint(fsInfo *fs.RegInfo, configName, root string, connConfig configmap.Simple) string {
	resolved := fs.ConfigMap(fsInfo.Prefix, fsInfo.Options, configName, connConfig)
	h := sha256.New()
	write := func(k, v string) { fmt.Fprintf(h, "%s=%s\n", k, v) }
	write("_backend", fsInfo.Name)
	write("_root", root)
	for _, opt := range fsInfo.Options {
		value, ok := resolved.Get(opt.Name)
		if !ok {
			value = ""
		}
		if (opt.IsPassword || opt.Sensitive) && value != "" {
			value = "<set>"
		}
		write(opt.Name, value)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// authContext hashes the info about the connected user.
//
// It returns "" for backends which don't assert their identity or
// when the info cannot be fetched.
func authContext(ctx context.Context, feats *fs.Features) string {
	if feats.UserInfo == nil {
		return ""
	}
	info, err := feats.UserInfo(ctx)
	if err != nil || len(info) == 0 {
		return ""
	}
	keys := make([]string, 0, len(info))
	for k := range info {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%s\n", k, info[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// build snapshots the current capabilities of f for k.
func build(ctx context.Context, f fs.Fs, k Key) *Profile {
	feats := f.Features()
	p := &Profile{
		Key:       k,
		Name:      f.Name(),
		Root:      f.Root(),
		Features:  feats.Enabled(),
		Precision: f.Precision(),
		FetchedAt: time.Now(),
		Hashes:    []string{},
	}
	for _, hashType := range f.Hashes().Array() {
		p.Hashes = append(p.Hashes, hashType.String())
	}
	if Enabled(ctx) {
		if ttl := time.Duration(fs.GetConfig(ctx).CapabilityCacheTTL); ttl > 0 {
			p.ExpiresAt = p.FetchedAt.Add(ttl)
		}
	}
	return p
}

var probeGroup singleflight.Group

// Get returns the capability profile for f, from the cache when fresh
// or by probing the remote and storing the result.
//
// When the cache is disabled this probes immediately, stores nothing
// and returns an error if probing the configuration fails.
func Get(ctx context.Context, f fs.Fs) (*Profile, error) {
	k, err := localKey(f)
	if err != nil {
		return nil, fmt.Errorf("capability cache: failed to identify configuration: %w", err)
	}
	if Enabled(ctx) {
		pruneExpired(k)
		if p := lookup(k, false); p != nil {
			return p, nil
		}
		k.withAuth(ctx, f)
		if p := lookup(k, true); p != nil {
			return p, nil
		}
		value, err, _ := probeGroup.Do(k.String(), func() (any, error) {
			p := build(ctx, f, k)
			return p, put(p)
		})
		if err != nil {
			return nil, fmt.Errorf("capability cache: probe failed: %w", err)
		}
		return value.(*Profile), nil
	}
	k.withAuth(ctx, f)
	return build(ctx, f, k), nil
}

// Probe snapshots the current capabilities of f without consulting or
// updating the cache, regardless of whether it is enabled.
func Probe(ctx context.Context, f fs.Fs) (*Profile, error) {
	k, err := localKey(f)
	if err != nil {
		return nil, fmt.Errorf("capability cache: failed to identify configuration: %w", err)
	}
	k.withAuth(ctx, f)
	return build(ctx, f, k), nil
}

// Refresh re-probes f and stores the new profile, bypassing TTL.
//
// When the cache is disabled this probes immediately and stores nothing.
func Refresh(ctx context.Context, f fs.Fs) (*Profile, error) {
	k, err := localKey(f)
	if err != nil {
		return nil, fmt.Errorf("capability cache: failed to identify configuration: %w", err)
	}
	k.withAuth(ctx, f)
	p := build(ctx, f, k)
	if Enabled(ctx) {
		if err := put(p); err != nil {
			return nil, fmt.Errorf("capability cache: refresh failed: %w", err)
		}
	}
	return p, nil
}

// Invalidate removes the stored profile for f.
//
// It returns true if an entry was removed. Configuration changes are
// detected automatically via the fingerprint, but this can be used to
// force a fresh probe regardless.
func Invalidate(ctx context.Context, f fs.Fs) (deleted bool, err error) {
	k, err := localKey(f)
	if err != nil {
		return false, fmt.Errorf("capability cache: failed to identify configuration: %w", err)
	}
	k.withAuth(ctx, f)
	deleted, err = remove(k)
	if err != nil {
		return deleted, fmt.Errorf("capability cache: invalidate failed: %w", err)
	}
	return deleted, nil
}

// InvalidateBackend removes all profiles for the backend type, e.g. "s3".
func InvalidateBackend(backend string) (deleted int, err error) {
	deleted, err = deleteBackend(backend)
	if err != nil {
		return deleted, fmt.Errorf("capability cache: invalidate failed: %w", err)
	}
	return deleted, nil
}

// Clear removes every profile from the cache.
func Clear() error {
	if err := clear(); err != nil {
		return fmt.Errorf("capability cache: clear failed: %w", err)
	}
	return nil
}

// List returns all fresh profiles in the cache, sorted by key.
func List() []*Profile {
	return all()
}
