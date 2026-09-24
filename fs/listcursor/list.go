package listcursor

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/dirtree"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/walk"
)

// errPageFull is returned by the page collecting callback when it has
// gathered PageSize entries. It is an internal signal, never returned
// to callers of this package.
var errPageFull = errors.New("list cursor page full")

// NativeSupported reports whether f resumes listings with a backend
// native continuation token under the current context and options.
//
// It mirrors the conditions under which walk.ListR uses the backend
// ListR directly, and refuses native paging for bucket based remotes
// when directories are requested: synthesized parent directories are
// only known after the full listing, so they cannot be paged in a
// stable order.
func NativeSupported(ctx context.Context, f fs.Fs, opt Options) bool {
	if f.Features().ListRCursor == nil {
		return false
	}
	if opt.MaxLevel >= 0 {
		return false
	}
	fi := filter.GetConfig(ctx)
	if fi.HaveFilesFrom() || len(fi.Opt.ExcludeFile) > 0 || fi.UsesDirectoryFilters() {
		return false
	}
	if f.Features().BucketBased && opt.ListType.Dirs() {
		return false
	}
	return true
}

// ListPage lists one page of the directory at path.
//
// If cur is nil the listing starts from the beginning, otherwise it
// resumes exactly after cur. The returned entries are sorted by
// Remote. When next is nil the listing is complete; a non nil next is
// the continuation cursor for the following page.
//
// Resuming a stale cursor fails loudly with ErrCursorExpired or
// ErrCursorInvalid rather than repeating or skipping entries.
func ListPage(ctx context.Context, f fs.Fs, path string, opt Options, cur *Cursor) (entries fs.DirEntries, next *Cursor, err error) {
	if opt.ListType == 0 {
		opt.ListType = walk.ListAll
	}
	if cur != nil {
		if err = cur.Validate(ctx, f, path, opt); err != nil {
			return nil, nil, err
		}
	}
	if cur != nil && cur.Native && !NativeSupported(ctx, f, opt) {
		return nil, nil, fmt.Errorf("%w: backend continuation token is no longer supported", ErrCursorInvalid)
	}
	if NativeSupported(ctx, f, opt) && (cur == nil || cur.Native) {
		return listPageNative(ctx, f, path, opt, cur)
	}
	if cur != nil && cur.Token != "" {
		return nil, nil, fmt.Errorf("%w: native continuation token cannot be resumed on this backend", ErrCursorInvalid)
	}
	return listPageAnchor(ctx, f, path, opt, cur)
}

// listPageNative pages a backend which implements fs.ListRCursorer.
func listPageNative(ctx context.Context, f fs.Fs, path string, opt Options, cur *Cursor) (page fs.DirEntries, next *Cursor, err error) {
	inputToken := ""
	var boundary string
	pageNo := 1
	if cur != nil {
		inputToken = cur.Token
		boundary = cur.LastKey
		pageNo = cur.Page + 1
	}
	resume := &fs.ListRResume{Token: inputToken, PageSize: opt.PageSize}
	pageFull := false
	rctx := fs.WithListRResume(ctx, resume)
	err = walk.ListR(rctx, f, path, opt.IncludeAll, opt.MaxLevel, opt.ListType, func(entries fs.DirEntries) error {
		for _, entry := range entries {
			// Defensive overlap guard for backends which return
			// the boundary entry again; the contract says exclusive.
			if boundary != "" && entry.Remote() <= boundary {
				continue
			}
			page = append(page, entry)
			if opt.PageSize > 0 && len(page) >= opt.PageSize {
				pageFull = true
				return errPageFull
			}
		}
		return nil
	})
	if err != nil && !errors.Is(err, errPageFull) {
		return nil, nil, err
	}
	// Native pages must be delivered in a stable order; sort defensively.
	sort.SliceStable(page, func(i, j int) bool { return page[i].Remote() < page[j].Remote() })
	if len(page) == 0 {
		if resume.More {
			if resume.Token == inputToken {
				return nil, nil, fmt.Errorf("%w: backend returned an empty page without advancing the continuation token", ErrCursorInvalid)
			}
			// Empty intermediate page: hand back the advanced token.
			next, err = newCursor(ctx, f, path, opt)
			if err != nil {
				return nil, nil, err
			}
			next.Native = true
			next.Token = resume.Token
			next.Page = pageNo
			return nil, next, nil
		}
		return nil, nil, nil
	}
	finished := !resume.More && !pageFull
	if !finished {
		next, err = newCursor(ctx, f, path, opt)
		if err != nil {
			return nil, nil, err
		}
		last := page[len(page)-1]
		next.Native = true
		next.Token = resume.Token
		next.LastKey = last.Remote()
		next.LastFp = boundaryFingerprint(ctx, last)
		next.Page = pageNo
	}
	return page, next, nil
}

// listPageAnchor pages using a complete, sorted DirTree and a path
// boundary. Resuming rebuilds the tree and skips up to and including
// the boundary, requiring the boundary entry to be present and
// unchanged.
func listPageAnchor(ctx context.Context, f fs.Fs, path string, opt Options, cur *Cursor) (page fs.DirEntries, next *Cursor, err error) {
	var lastEntry fs.DirEntry
	pageNo := 1
	if cur != nil {
		pageNo = cur.Page + 1
	}
	pageFull := false
	sawAnchor, err := scanAnchorTree(ctx, f, path, opt, cur, func(entry fs.DirEntry) error {
		lastEntry = entry
		page = append(page, entry)
		if opt.PageSize > 0 && len(page) >= opt.PageSize {
			pageFull = true
			return errPageFull
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if cur != nil && !sawAnchor {
		return nil, nil, fmt.Errorf("%w: boundary entry %q not found, refusing to resume", ErrCursorInvalid, cur.LastKey)
	}
	if len(page) == 0 {
		return nil, nil, nil
	}
	if pageFull {
		next, err = newCursor(ctx, f, path, opt)
		if err != nil {
			return nil, nil, err
		}
		next.LastKey = lastEntry.Remote()
		next.LastFp = boundaryFingerprint(ctx, lastEntry)
		next.Page = pageNo
	}
	return page, next, nil
}

// NewDirTreePage builds a dirtree.DirTree containing the entries at
// and after cur in the same deterministic order walk.NewDirTree uses.
//
// Parent directories of remaining entries are retained as
// scaffolding, so the tree can be rendered directly; entries at or
// before the boundary are omitted. A native cursor cannot be consumed
// here (it must continue through ListPage).
func NewDirTreePage(ctx context.Context, f fs.Fs, path string, includeAll bool, maxLevel int, cur *Cursor) (dirtree.DirTree, *Cursor, error) {
	opt := Options{
		IncludeAll: includeAll,
		MaxLevel:   maxLevel,
		ListType:   walk.ListAll,
	}
	if cur != nil {
		if err := cur.Validate(ctx, f, path, opt); err != nil {
			return nil, nil, err
		}
		if cur.Native {
			return nil, nil, fmt.Errorf("%w: native continuation cursor must be resumed with ListPage", ErrCursorInvalid)
		}
	}
	out := dirtree.New()
	sawAnchor, err := scanAnchorTree(ctx, f, path, opt, cur, func(entry fs.DirEntry) error {
		out.AddEntry(entry)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if cur != nil && !sawAnchor {
		return nil, nil, fmt.Errorf("%w: boundary entry %q not found, refusing to resume", ErrCursorInvalid, cur.LastKey)
	}
	if _, found := out[path]; !found {
		out[path] = nil
	}
	return out, nil, nil
}

// scanAnchorTree builds a sorted DirTree, flattens it globally by
// Remote and calls visit for entries at and after the cursor
// boundary, applying the requested list type. It reports whether the
// boundary was seen (true when cur is nil) and stops as soon as visit
// returns errPageFull.
//
// The flattened order is global lexicographical order, not the
// per-directory walk order, so the same LastKey boundary works from
// any page and matches the order native backends page in.
func scanAnchorTree(ctx context.Context, f fs.Fs, path string, opt Options, cur *Cursor, visit func(fs.DirEntry) error) (bool, error) {
	dirs, err := walk.NewDirTree(ctx, f, path, opt.IncludeAll, opt.MaxLevel)
	if err != nil {
		return false, err
	}
	all := make(fs.DirEntries, 0)
	for _, dirPath := range dirs.Dirs() {
		all = append(all, dirs[dirPath]...)
	}
	sort.Stable(all)
	skipping := cur != nil
	sawAnchor := cur == nil
	for _, entry := range all {
		switch entry.(type) {
		case fs.Object:
			if !opt.ListType.Objects() {
				continue
			}
		case fs.Directory:
			if !opt.ListType.Dirs() {
				continue
			}
		default:
			return false, fmt.Errorf("unknown object type %T", entry)
		}
		if skipping {
			key := entry.Remote()
			switch {
			case key < cur.LastKey:
				// Re-read prefix from the rebuilt tree; ignore.
				continue
			case key == cur.LastKey:
				if cur.LastFp != "" {
					if fp := boundaryFingerprint(ctx, entry); fp != cur.LastFp {
						return false, fmt.Errorf("%w: boundary entry %q changed since the cursor was made", ErrCursorInvalid, cur.LastKey)
					}
				}
				skipping = false
				sawAnchor = true
				// The boundary itself was delivered on the previous page.
				continue
			default:
				return false, fmt.Errorf("%w: boundary entry %q not found, refusing to resume", ErrCursorInvalid, cur.LastKey)
			}
		}
		if err := visit(entry); err != nil {
			if errors.Is(err, errPageFull) {
				return sawAnchor, nil
			}
			return false, err
		}
	}
	return sawAnchor, nil
}

// boundaryFingerprint captures enough of a boundary DirEntry to
// detect a modification while resuming.
//
// Object attributes are fingerprinted with fs.Fingerprint. A
// directory only proves the boundary type and existence: directory
// metadata doesn't affect lexicographical ordering and synthesized
// directories carry no stable modtime, so comparing attributes would
// reject otherwise identical listings.
func boundaryFingerprint(ctx context.Context, entry fs.DirEntry) string {
	switch x := entry.(type) {
	case fs.Object:
		return "o:" + fs.Fingerprint(ctx, x, true)
	case fs.Directory:
		return "d"
	}
	return ""
}
