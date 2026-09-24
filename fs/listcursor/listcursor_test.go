package listcursor_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/rclone/rclone/backend/memory"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/filter"
	"github.com/rclone/rclone/fs/listcursor"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/fstest/mockobject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMemoryBucketFs returns a memory Fs rooted at a unique bucket.
func newMemoryBucketFs(t *testing.T, ctx context.Context, suffix string) fs.Fs {
	t.Helper()
	name := fmt.Sprintf("lc-test-%s-%d", suffix, time.Now().UnixNano())
	f, err := fs.NewFs(ctx, ":memory:"+name)
	if errors.Is(err, fs.ErrorDirNotFound) {
		rootF, err2 := fs.NewFs(ctx, ":memory:")
		require.NoError(t, err2)
		require.NoError(t, rootF.Mkdir(ctx, name))
		f, err = fs.NewFs(ctx, ":memory:"+name)
	}
	require.NoError(t, err)
	return f
}

func putFile(t *testing.T, ctx context.Context, f fs.Fs, remote, content string) fs.Object {
	t.Helper()
	src := mockobject.New(remote).WithContent([]byte(content), mockobject.SeekModeNone)
	src.SetFs(f)
	in, err := src.Open(ctx)
	require.NoError(t, err)
	o, err := f.Put(ctx, in, src)
	require.NoError(t, err)
	return o
}

func objectRemotes(t *testing.T, ctx context.Context, f fs.Fs) []string {
	t.Helper()
	objs, dirs, err := walk.GetAll(ctx, f, "", true, -1)
	require.NoError(t, err)
	remotes := make([]string, 0, len(objs)+len(dirs))
	for _, o := range objs {
		remotes = append(remotes, "o:"+o.Remote())
	}
	for _, d := range dirs {
		remotes = append(remotes, "d:"+d.Remote())
	}
	sort.Strings(remotes)
	return remotes
}

// pageAll pages through the whole listing starting from cur,
// returning every typed remote and the final cursor (nil when the
// listing completed).
func pageAll(t *testing.T, ctx context.Context, f fs.Fs, opt listcursor.Options, cur *listcursor.Cursor) ([]string, *listcursor.Cursor) {
	t.Helper()
	var got []string
	for {
		entries, next, err := listcursor.ListPage(ctx, f, "", opt, cur)
		require.NoError(t, err)
		for _, e := range entries {
			switch e.(type) {
			case fs.Object:
				got = append(got, "o:"+e.Remote())
			case fs.Directory:
				got = append(got, "d:"+e.Remote())
			}
		}
		cur = next
		if cur == nil {
			return got, nil
		}
	}
}

func TestCursorEncodeDecode(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "encode")
	putFile(t, ctx, f, "a.txt", "a")

	cur, err := listcursor.DecodeCursor("")
	require.NoError(t, err)
	require.Nil(t, cur)

	cur = &listcursor.Cursor{Version: 1}
	encoded, err := cur.Encode()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(encoded, "rclc1_"))

	decoded, err := listcursor.DecodeCursor(encoded)
	require.NoError(t, err)
	assert.Equal(t, 1, decoded.Version)

	_, err = listcursor.DecodeCursor("garbage")
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
	_, err = listcursor.DecodeCursor("rclc1_garbage!!!")
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestListPageNative(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "native")
	files := []string{"a.txt", "b.txt", "c.txt", "sub/d.txt", "sub/e.txt", "z.txt"}
	for i, name := range files {
		putFile(t, ctx, f, name, fmt.Sprintf("content-%d", i))
	}
	assert.True(t, listcursor.NativeSupported(ctx, f, listcursor.Options{
		MaxLevel: -1,
		ListType: walk.ListObjects,
	}))
	// Bucket based remotes can't page synthesized directories natively.
	assert.False(t, listcursor.NativeSupported(ctx, f, listcursor.Options{
		MaxLevel: -1,
		ListType: walk.ListAll,
	}))

	opt := listcursor.Options{
		IncludeAll: true,
		MaxLevel:   -1,
		ListType:   walk.ListObjects,
		PageSize:   2,
	}
	got, final := pageAll(t, ctx, f, opt, nil)
	assert.Nil(t, final)
	assert.Equal(t, []string{
		"o:a.txt", "o:b.txt", "o:c.txt", "o:sub/d.txt", "o:sub/e.txt", "o:z.txt",
	}, got)

	// Paging through the same listing must deliver every object
	// exactly once even compared with a plain complete listing.
	objs, _, err := walk.GetAll(ctx, f, "", true, -1)
	require.NoError(t, err)
	all := make([]string, 0, len(objs))
	for _, o := range objs {
		all = append(all, o.Remote())
	}
	sort.Strings(all)
	plain := make([]string, len(got))
	for i, s := range got {
		plain[i] = strings.TrimPrefix(s, "o:")
	}
	assert.Equal(t, all, plain)
}

func TestListPageNativeResumeAcrossCalls(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "native-resume")
	files := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	for i, name := range files {
		putFile(t, ctx, f, name, fmt.Sprintf("content-%d", i))
	}
	opt := listcursor.Options{IncludeAll: true, MaxLevel: -1, ListType: walk.ListObjects, PageSize: 2}

	// First call, then pretend the process died and resume from the
	// encoded cursor in a brand new call.
	page1, cur1, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur1)
	assert.True(t, cur1.Native)
	assert.Len(t, page1, 2)

	encoded, err := cur1.Encode()
	require.NoError(t, err)
	cur1b, err := listcursor.DecodeCursor(encoded)
	require.NoError(t, err)

	rest, final := pageAll(t, ctx, f, opt, cur1b)
	assert.Nil(t, final)
	assert.Equal(t, []string{"o:c.txt", "o:d.txt", "o:e.txt"}, rest)
}

func TestListPageAnchor(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "anchor")
	files := []string{"a.txt", "b.txt", "c.txt", "sub/d.txt", "sub/e.txt", "z.txt"}
	for i, name := range files {
		putFile(t, ctx, f, name, fmt.Sprintf("content-%d", i))
	}
	// Bounded recursion forces the path boundary fallback on the
	// memory backend even though it has a native continuation token.
	opt := listcursor.Options{
		IncludeAll: true,
		MaxLevel:   1,
		ListType:   walk.ListObjects,
		PageSize:   2,
	}
	assert.False(t, listcursor.NativeSupported(ctx, f, opt))
	got, final := pageAll(t, ctx, f, opt, nil)
	assert.Nil(t, final)
	assert.Equal(t, []string{"o:a.txt", "o:b.txt", "o:c.txt", "o:z.txt"}, got)

	// Unlimited listing including directories also uses the anchor
	// path for bucket based backends and yields the full tree.
	optAll := listcursor.Options{
		IncludeAll: true,
		MaxLevel:   -1,
		ListType:   walk.ListAll,
		PageSize:   3,
	}
	gotAll, final := pageAll(t, ctx, f, optAll, nil)
	assert.Nil(t, final)
	sort.Strings(gotAll)
	assert.Equal(t, objectRemotes(t, ctx, f), gotAll)
}

func TestListPageAnchorResume(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "anchor-resume")
	files := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	for i, name := range files {
		putFile(t, ctx, f, name, fmt.Sprintf("content-%d", i))
	}
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 2}

	page1, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.False(t, cur.Native)
	assert.Equal(t, "b.txt", cur.LastKey)
	assert.Equal(t, []string{"o:a.txt", "o:b.txt"}, typedRemotes(page1))

	rest, final := pageAll(t, ctx, f, opt, cur)
	assert.Nil(t, final)
	assert.Equal(t, []string{"o:c.txt", "o:d.txt", "o:e.txt"}, rest)
}

func TestListPageAnchorMissingBoundary(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "anchor-missing")
	objects := map[string]fs.Object{}
	for i, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		objects[name] = putFile(t, ctx, f, name, fmt.Sprintf("content-%d", i))
	}
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 2}
	_, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, "b.txt", cur.LastKey)

	// Delete the boundary entry: resuming must fail loudly instead
	// of silently skipping over the gap.
	require.NoError(t, objects["b.txt"].Remove(ctx))
	_, _, err = listcursor.ListPage(ctx, f, "", opt, cur)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestListPageAnchorChangedBoundary(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "anchor-changed")
	putFile(t, ctx, f, "a.txt", "a")
	boundary := putFile(t, ctx, f, "b.txt", "b")
	putFile(t, ctx, f, "c.txt", "c")
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 2}
	_, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)

	// Change the size of the boundary object: its fingerprint no
	// longer matches so the cursor is rejected.
	src := mockobject.New("b.txt").WithContent([]byte("b-changed-to-a-longer-body"), mockobject.SeekModeNone)
	src.SetFs(f)
	in, err := src.Open(ctx)
	require.NoError(t, err)
	require.NoError(t, boundary.Update(ctx, in, src))

	_, _, err = listcursor.ListPage(ctx, f, "", opt, cur)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestCursorExpired(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "expired")
	putFile(t, ctx, f, "a.txt", "a")
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 1}
	_, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)

	cur.Expires = time.Now().Add(-time.Minute)
	encoded, err := cur.Encode()
	require.NoError(t, err)
	old, err := listcursor.DecodeCursor(encoded)
	require.NoError(t, err)
	_, _, err = listcursor.ListPage(ctx, f, "", opt, old)
	assert.ErrorIs(t, err, listcursor.ErrCursorExpired)
}

func TestCursorFilterFingerprintMismatch(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "filter-fp")
	putFile(t, ctx, f, "a.txt", "a")
	putFile(t, ctx, f, "b.log", "b")
	opt := listcursor.Options{IncludeAll: false, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 1}

	page1, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	require.Len(t, page1, 1)

	// Change the active filter in between the two calls.
	fctx, fi := filter.AddConfig(ctx)
	require.NoError(t, fi.AddRule("+ *.log"))
	require.NoError(t, fi.AddRule("- *"))
	_, _, err = listcursor.ListPage(fctx, f, "", opt, cur)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestCursorConfigFingerprintMismatch(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "config-fp")
	putFile(t, ctx, f, "a.txt", "a")
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 1}
	_, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)

	cctx, ci := fs.AddConfig(ctx)
	ci.IgnoreCaseSync = !ci.IgnoreCaseSync
	_, _, err = listcursor.ListPage(cctx, f, "", opt, cur)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestCursorPathMismatch(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "path-mismatch")
	putFile(t, ctx, f, "sub/s.txt", "s")
	putFile(t, ctx, f, "other/o.txt", "o")
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 1}
	_, cur, err := listcursor.ListPage(ctx, f, "sub", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)

	_, _, err = listcursor.ListPage(ctx, f, "other", opt, cur)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestCursorNoDuplicatesWhenRestarted(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "no-dups")
	files := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt", "f.txt"}
	for i, name := range files {
		putFile(t, ctx, f, name, fmt.Sprintf("content-%d", i))
	}
	opt := listcursor.Options{IncludeAll: true, MaxLevel: 1, ListType: walk.ListObjects, PageSize: 3}

	// Deliver page 1, die, repeat the resume call with the same
	// cursor: the boundary entry must never be delivered twice.
	_, cur1, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur1)

	var advanced *listcursor.Cursor
	for i := 0; i < 3; i++ {
		page, next, err := listcursor.ListPage(ctx, f, "", opt, cur1)
		require.NoError(t, err)
		require.NotNil(t, next)
		assert.Equal(t, []string{"o:d.txt", "o:e.txt", "o:f.txt"}, typedRemotes(page))
		advanced = next
	}
	// Continuing from the cursor handed back by a repeated page
	// reaches the end exactly once.
	page, done, err := listcursor.ListPage(ctx, f, "", opt, advanced)
	require.NoError(t, err)
	assert.Nil(t, done)
	assert.Empty(t, typedRemotes(page))
}

func TestNewDirTreePage(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "dirtree")
	putFile(t, ctx, f, "a.txt", "a")
	putFile(t, ctx, f, "sub/b.txt", "b")
	putFile(t, ctx, f, "sub/deep/c.txt", "c")
	putFile(t, ctx, f, "z.txt", "z")
	opt := listcursor.Options{IncludeAll: true, MaxLevel: -1, ListType: walk.ListAll, PageSize: 2}

	_, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.Equal(t, "sub", cur.LastKey)

	tree, next, err := listcursor.NewDirTreePage(ctx, f, "", true, -1, cur)
	require.NoError(t, err)
	assert.Nil(t, next)

	// The remaining entries are present with parent scaffolding...
	_, bEntry := tree.Find("sub/b.txt")
	require.NotNil(t, bEntry)
	_, cEntry := tree.Find("sub/deep/c.txt")
	require.NotNil(t, cEntry)
	_, zEntry := tree.Find("z.txt")
	require.NotNil(t, zEntry)
	// ...while delivered entries are gone.
	_, aEntry := tree.Find("a.txt")
	assert.Nil(t, aEntry)
	_, root := tree[""]
	assert.NotNil(t, root)
}

func TestNewDirTreePageRejectsNativeCursor(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "dirtree-native")
	putFile(t, ctx, f, "a.txt", "a")
	opt := listcursor.Options{IncludeAll: true, MaxLevel: -1, ListType: walk.ListObjects, PageSize: 1}
	_, cur, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	assert.True(t, cur.Native)

	_, _, err = listcursor.NewDirTreePage(ctx, f, "", true, -1, cur)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

func TestListPageDefaultOrdering(t *testing.T) {
	ctx := context.Background()
	f := newMemoryBucketFs(t, ctx, "ordering")
	names := []string{"m.txt", "a.txt", "z.txt", "k.txt"}
	for _, name := range names {
		putFile(t, ctx, f, name, name)
	}
	// PageSize 0 hands back the backend native page, which must
	// still be in stable sorted order.
	opt := listcursor.Options{IncludeAll: true, MaxLevel: -1, ListType: walk.ListObjects}
	page, next, err := listcursor.ListPage(ctx, f, "", opt, nil)
	require.NoError(t, err)
	assert.Nil(t, next)
	assert.Equal(t, []string{"o:a.txt", "o:k.txt", "o:m.txt", "o:z.txt"}, typedRemotes(page))
}

func typedRemotes(entries fs.DirEntries) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		switch e.(type) {
		case fs.Object:
			out = append(out, "o:"+e.Remote())
		case fs.Directory:
			out = append(out, "d:"+e.Remote())
		}
	}
	sort.Strings(out)
	return out
}
