// Package listcursor provides resumable, paged directory listings.
//
// A Cursor is an opaque, self describing continuation token. It binds
// a listing page to the exact state it was started from: the backend
// and root, the listing path, the recursion depth, the active
// include/exclude filter and the listing relevant configuration.
// That state is fingerprinted so a cursor can only ever resume the
// listing it was created for.
//
// Backends which implement fs.ListRCursorer are paged with their
// native continuation token. Every other backend falls back to a
// stable, sorted listing with a path boundary: resuming re-reads up
// to the boundary, verifies it is unchanged, and only then continues,
// so entries are neither silently repeated nor skipped.
package listcursor

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/walk"
)

// Errors returned for cursors which can no longer be used. Callers
// should restart the listing from the beginning instead of retrying.
var (
	// ErrCursorInvalid is returned when a cursor fails validation:
	// it was tampered with, its fingerprint no longer matches the
	// backend, path, filter or configuration, or its boundary can't
	// be found.
	ErrCursorInvalid = errors.New("list cursor invalid")

	// ErrCursorExpired is returned when a cursor is resumed after
	// its expiry time.
	ErrCursorExpired = errors.New("list cursor expired")
)

const (
	cursorVersion = 1
	// cursorPrefix makes encoded cursors recognisable in logs and
	// command lines.
	cursorPrefix = "rclc1_"
	// DefaultTTL is the lifetime assigned to a cursor when Options.TTL
	// is zero.
	DefaultTTL = 24 * time.Hour
)

// Options controls a paged listing.
type Options struct {
	// IncludeAll disables the active filter, as in walk.ListR.
	IncludeAll bool
	// MaxLevel is the recursion depth, -1 for unlimited.
	MaxLevel int
	// ListType selects objects, directories or both.
	ListType walk.ListType
	// PageSize is the maximum number of entries delivered per page.
	// A value of 0 uses the backend native page size or delivers the
	// whole remaining listing on the fallback path.
	PageSize int
	// TTL is the lifetime of cursors created for this listing. Zero
	// means DefaultTTL.
	TTL time.Duration
}

// Cursor is a self describing continuation token for a paged listing.
//
// It is JSON serialized and base64 (URL) encoded with Encode; the
// content is readable but must be treated as opaque by callers.
type Cursor struct {
	// Version is the cursor format version.
	Version int `json:"v"`
	// Backend is the Name() of the Fs the cursor was made for.
	Backend string `json:"backend"`
	// Root is the Root() of the Fs the cursor was made for.
	Root string `json:"root"`
	// Path is the listing start path.
	Path string `json:"path"`
	// IncludeAll, MaxLevel and ListTypeName pin the listing shape.
	IncludeAll   bool   `json:"includeAll"`
	MaxLevel     int    `json:"maxLevel"`
	ListTypeName string `json:"listType"`
	FilterFp     string `json:"filterFp"`
	ConfigFp     string `json:"configFp"`
	// Token is the backend native continuation token. It is empty
	// on the path boundary fallback.
	Token string `json:"token,omitempty"`
	// Native marks a cursor driven by a backend continuation token.
	Native bool `json:"native"`
	// LastKey is the Remote() of the last entry already delivered;
	// it is the path boundary used to resume and detect overlap.
	LastKey string `json:"lastKey,omitempty"`
	// LastFp fingerprints the boundary entry so a changed boundary
	// invalidates the cursor.
	LastFp string `json:"lastFp,omitempty"`
	// Created and Expires bound the cursor lifetime.
	Created time.Time `json:"created"`
	Expires time.Time `json:"expires"`
	// Page counts delivered pages starting at 1.
	Page int `json:"page"`
}

func listTypeName(t walk.ListType) string {
	switch t {
	case walk.ListObjects:
		return "objects"
	case walk.ListDirs:
		return "dirs"
	case walk.ListAll:
		return "all"
	}
	return fmt.Sprintf("unknown(%d)", int(t))
}

func newCursor(ctx context.Context, f fs.Info, path string, opt Options) (*Cursor, error) {
	now := time.Now()
	ttl := opt.TTL
	if ttl == 0 {
		ttl = DefaultTTL
	}
	filterFp, err := filterFingerprint(ctx)
	if err != nil {
		return nil, err
	}
	return &Cursor{
		Version:      cursorVersion,
		Backend:      f.Name(),
		Root:         f.Root(),
		Path:         path,
		IncludeAll:   opt.IncludeAll,
		MaxLevel:     opt.MaxLevel,
		ListTypeName: listTypeName(opt.ListType),
		FilterFp:     filterFp,
		ConfigFp:     configFingerprint(ctx, f),
		Created:      now,
		Expires:      now.Add(ttl),
	}, nil
}

// Encode serializes the cursor into an opaque URL safe string.
func (c *Cursor) Encode() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode list cursor: %w", err)
	}
	return cursorPrefix + base64.RawURLEncoding.EncodeToString(data), nil
}

// DecodeCursor decodes a cursor produced by Encode. An empty input
// returns a nil cursor and nil error, so callers can pass an unset
// option straight through.
func DecodeCursor(s string) (*Cursor, error) {
	if s == "" {
		return nil, nil
	}
	rest, ok := trimPrefix(s, cursorPrefix)
	if !ok {
		return nil, fmt.Errorf("%w: unrecognised cursor encoding", ErrCursorInvalid)
	}
	data, err := base64.RawURLEncoding.DecodeString(rest)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor encoding: %v", ErrCursorInvalid, err)
	}
	c := &Cursor{}
	if err := json.Unmarshal(data, c); err != nil {
		return nil, fmt.Errorf("%w: cursor encoding: %v", ErrCursorInvalid, err)
	}
	if c.Version != cursorVersion {
		return nil, fmt.Errorf("%w: unsupported cursor version %d", ErrCursorInvalid, c.Version)
	}
	return c, nil
}

func trimPrefix(s, prefix string) (string, bool) {
	if len(s) < len(prefix) || s[:len(prefix)] != prefix {
		return "", false
	}
	return s[len(prefix):], true
}

// Validate checks that the cursor can still resume a listing with
// the given shape against f, returning ErrCursorExpired or
// ErrCursorInvalid wrapped with the reason otherwise.
//
// The ListType is only checked when opt.ListType is non zero, so
// callers which consume a listing without that distinction (e.g.
// march) may leave it unset.
func (c *Cursor) Validate(ctx context.Context, f fs.Info, path string, opt Options) error {
	if c.Version != cursorVersion {
		return fmt.Errorf("%w: unsupported cursor version %d", ErrCursorInvalid, c.Version)
	}
	if now := time.Now(); now.After(c.Expires) {
		return fmt.Errorf("%w: cursor expired at %s", ErrCursorExpired, c.Expires.Format(time.RFC3339))
	}
	if c.Backend != f.Name() || c.Root != f.Root() {
		return fmt.Errorf("%w: cursor was made for %q:%q not %q:%q", ErrCursorInvalid, c.Backend, c.Root, f.Name(), f.Root())
	}
	if c.Path != path {
		return fmt.Errorf("%w: cursor path %q does not match %q", ErrCursorInvalid, c.Path, path)
	}
	if c.IncludeAll != opt.IncludeAll {
		return fmt.Errorf("%w: cursor includeAll flag changed", ErrCursorInvalid)
	}
	if c.MaxLevel != opt.MaxLevel {
		return fmt.Errorf("%w: cursor max depth %d does not match %d", ErrCursorInvalid, c.MaxLevel, opt.MaxLevel)
	}
	if opt.ListType != 0 && c.ListTypeName != listTypeName(opt.ListType) {
		return fmt.Errorf("%w: cursor list type %s does not match %s", ErrCursorInvalid, c.ListTypeName, listTypeName(opt.ListType))
	}
	filterFp, err := filterFingerprint(ctx)
	if err != nil {
		return fmt.Errorf("%w: cannot compute filter fingerprint: %v", ErrCursorInvalid, err)
	}
	if c.FilterFp != filterFp {
		return fmt.Errorf("%w: active filter changed since the cursor was made", ErrCursorInvalid)
	}
	if c.ConfigFp != configFingerprint(ctx, f) {
		return fmt.Errorf("%w: listing configuration changed since the cursor was made", ErrCursorInvalid)
	}
	return nil
}

// configPart is the listing relevant subset of the global config
// which, together with the backend feature flags, forms the
// configuration fingerprint.
type configPart struct {
	UseListR        bool
	NoTraverse      bool
	ListCutoff      int
	IgnoreCaseSync  bool
	BucketBased     bool
	FilterAware     bool
	CaseInsensitive bool
	DuplicateFiles  bool
}

func configFingerprint(ctx context.Context, f fs.Info) string {
	ci := fs.GetConfig(ctx)
	part := configPart{
		UseListR:       ci.UseListR,
		NoTraverse:     ci.NoTraverse,
		ListCutoff:     ci.ListCutoff,
		IgnoreCaseSync: ci.IgnoreCaseSync,
	}
	if features := f.Features(); features != nil {
		part.BucketBased = features.BucketBased
		part.FilterAware = features.FilterAware
		part.CaseInsensitive = features.CaseInsensitive
		part.DuplicateFiles = features.DuplicateFiles
	}
	return fingerprint(part)
}

// filterFingerprint hashes the active filter options. The option
// slices hold the include/exclude rule strings, so rule changes
// change the fingerprint; --files-from style options contribute the
// file names given on the command line.
func filterFingerprint(ctx context.Context) (string, error) {
	fi := filter.GetConfig(ctx)
	data, err := json.Marshal(fi.Opt)
	if err != nil {
		return "", fmt.Errorf("marshal filter options: %w", err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func fingerprint(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		// configPart is a plain struct this can't fail for; fall
		// back to a constant rather than panicking in listings.
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
