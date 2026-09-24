package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/listcursor"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/walk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cursorMemFs(t *testing.T, ctx context.Context, role string) fs.Fs {
	t.Helper()
	name := fmt.Sprintf("sync-cursor-%s-%d", role, time.Now().UnixNano())
	f, err := fs.NewFs(ctx, ":memory:"+name)
	if errors.Is(err, fs.ErrorDirNotFound) {
		root, err2 := fs.NewFs(ctx, ":memory:")
		require.NoError(t, err2)
		require.NoError(t, root.Mkdir(ctx, name))
		f, err = fs.NewFs(ctx, ":memory:"+name)
	}
	require.NoError(t, err)
	return f
}

func putMemObject(t *testing.T, ctx context.Context, f fs.Fs, remote, content string) {
	t.Helper()
	_, err := operations.Rcat(ctx, f, remote, io.NopCloser(strings.NewReader(content)), t1, nil)
	require.NoError(t, err)
}

func objectNames(t *testing.T, ctx context.Context, f fs.Fs) []string {
	t.Helper()
	objs, _, err := walk.GetAll(ctx, f, "", true, -1)
	require.NoError(t, err)
	names := make([]string, 0, len(objs))
	for _, o := range objs {
		names = append(names, o.Remote())
	}
	return names
}

// anchorCursor pages src with the non-native path boundary path and
// returns the cursor after the first page.
func anchorCursor(t *testing.T, ctx context.Context, f fs.Fs, pageSize int) *listcursor.Cursor {
	t.Helper()
	// Listing directories on a bucket based backend forces the path
	// boundary fallback (native tokens only cover objects there).
	_, cur, err := listcursor.ListPage(ctx, f, "", listcursor.Options{
		IncludeAll: false,
		MaxLevel:   -1,
		ListType:   walk.ListAll,
		PageSize:   pageSize,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	require.False(t, cur.Native)
	return cur
}

// Copying after a list cursor skips the already processed boundary
// prefix and finishes the copy without repeating it.
func TestCopyResumeFromCursor(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	src := cursorMemFs(t, ctx, "src1")
	dst := cursorMemFs(t, ctx, "dst1")
	for i, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		putMemObject(t, ctx, src, name, fmt.Sprintf("content-%d", i))
	}

	// Simulate a previous interrupted run which copied the first
	// page (up to and including the b.txt boundary).
	for _, name := range []string{"a.txt", "b.txt"} {
		require.NoError(t, operations.CopyFile(ctx, dst, src, name, name))
	}

	cur := anchorCursor(t, ctx, src, 2)
	require.Equal(t, "b.txt", cur.LastKey)
	encoded, err := cur.Encode()
	require.NoError(t, err)
	ci.ListCursorIn = encoded
	require.NoError(t, CopyDir(ctx, dst, src, false))

	assert.ElementsMatch(t,
		[]string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"},
		objectNames(t, ctx, dst))
}

// Resuming with the next cursor later stays idempotent: already
// copied objects are matched, only the tail is transferred.
func TestCopyResumeFromCursorAgain(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	src := cursorMemFs(t, ctx, "src2")
	dst := cursorMemFs(t, ctx, "dst2")
	for i, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"} {
		putMemObject(t, ctx, src, name, fmt.Sprintf("content-%d", i))
	}
	cur := anchorCursor(t, ctx, src, 2)
	page2, next, err := listcursor.ListPage(ctx, src, "", listcursor.Options{
		IncludeAll: false,
		MaxLevel:   -1,
		ListType:   walk.ListAll,
		PageSize:   2,
	}, cur)
	require.NoError(t, err)
	require.NotNil(t, next)
	require.Len(t, page2, 2)
	require.Equal(t, "d.txt", next.LastKey)

	for _, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		require.NoError(t, operations.CopyFile(ctx, dst, src, name, name))
	}
	encoded, err := next.Encode()
	require.NoError(t, err)
	ci.ListCursorIn = encoded
	require.NoError(t, CopyDir(ctx, dst, src, false))

	assert.ElementsMatch(t,
		[]string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"},
		objectNames(t, ctx, dst))
}

// A boundary object which disappeared invalidates the cursor and the
// copy must fail instead of silently skipping the gap.
func TestCopyResumeMissingBoundary(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	src := cursorMemFs(t, ctx, "src3")
	dst := cursorMemFs(t, ctx, "dst3")
	objects := map[string]fs.Object{}
	for i, name := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		content := fmt.Sprintf("content-%d", i)
		putMemObject(t, ctx, src, name, content)
		o, err := src.NewObject(ctx, name)
		require.NoError(t, err)
		objects[name] = o
	}
	cur := anchorCursor(t, ctx, src, 2)
	require.NoError(t, objects["b.txt"].Remove(ctx))
	require.NoError(t, dst.Mkdir(ctx, ""))

	encoded, err := cur.Encode()
	require.NoError(t, err)
	ci.ListCursorIn = encoded
	err = CopyDir(ctx, dst, src, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
	assert.Empty(t, objectNames(t, ctx, dst))
}

// A boundary object whose content changed invalidates the cursor.
func TestCopyResumeChangedBoundary(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	src := cursorMemFs(t, ctx, "src4")
	dst := cursorMemFs(t, ctx, "dst4")
	putMemObject(t, ctx, src, "a.txt", "a")
	putMemObject(t, ctx, src, "b.txt", "b")
	putMemObject(t, ctx, src, "c.txt", "c")
	cur := anchorCursor(t, ctx, src, 2)
	putMemObject(t, ctx, src, "b.txt", "b-with-a-much-longer-body")

	encoded, err := cur.Encode()
	require.NoError(t, err)
	ci.ListCursorIn = encoded
	err = CopyDir(ctx, dst, src, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}

// Sync (which deletes) and move must refuse a resume cursor rather
// than applying a partial source view to the destination.
func TestSyncAndMoveRefuseCursor(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	src := cursorMemFs(t, ctx, "src5")
	dst := cursorMemFs(t, ctx, "dst5")
	putMemObject(t, ctx, src, "a.txt", "a")
	cur := anchorCursor(t, ctx, src, 1)
	encoded, err := cur.Encode()
	require.NoError(t, err)
	ci.ListCursorIn = encoded

	err = Sync(ctx, dst, src, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid, "sync must refuse a resume cursor")

	err = moveDir(ctx, dst, src, false, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid, "move must refuse a resume cursor")
}

// A native (backend token) cursor can't drive march either - it gets
// a loud invalidation, not a partial copy.
func TestCopyRejectsNativeCursor(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	src := cursorMemFs(t, ctx, "src6")
	dst := cursorMemFs(t, ctx, "dst6")
	putMemObject(t, ctx, src, "a.txt", "a")
	_, cur, err := listcursor.ListPage(ctx, src, "", listcursor.Options{
		IncludeAll: false,
		MaxLevel:   -1,
		ListType:   walk.ListObjects,
		PageSize:   1,
	}, nil)
	require.NoError(t, err)
	require.NotNil(t, cur)
	require.True(t, cur.Native)
	encoded, err := cur.Encode()
	require.NoError(t, err)
	ci.ListCursorIn = encoded

	err = CopyDir(ctx, dst, src, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, listcursor.ErrCursorInvalid)
}
