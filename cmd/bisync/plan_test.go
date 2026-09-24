package bisync

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/cmd/bisync/bilib"
	"github.com/rclone/rclone/fs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	_ "github.com/rclone/rclone/backend/local"
)

// fingerprint ----------------------------------------------------------------

func TestFileListFingerprintStable(t *testing.T) {
	ls := newFileList()
	when := time.Date(2021, time.June, 1, 12, 0, 0, 0, time.UTC)
	ls.put("b.txt", 2, when.Add(time.Second), "", "-", "-")
	ls.put("a.txt", 1, when, "", "-", "-")

	first := ls.fingerprint()
	second := ls.fingerprint()
	assert.Equal(t, first, second, "fingerprint must be deterministic")
	assert.Len(t, first, 64, "sha256 hex length")

	// insertion order must not change the fingerprint
	reordered := newFileList()
	reordered.put("a.txt", 1, when, "", "-", "-")
	reordered.put("b.txt", 2, when.Add(time.Second), "", "-", "-")
	assert.Equal(t, first, reordered.fingerprint(), "fingerprint must not depend on insertion order")

	// any change must change the fingerprint
	changed := newFileList()
	changed.put("a.txt", 1, when, "", "-", "-")
	changed.put("b.txt", 3, when.Add(time.Second), "", "-", "-")
	assert.NotEqual(t, first, changed.fingerprint(), "different size must change fingerprint")

	changed2 := newFileList()
	changed2.put("a.txt", 1, when, "", "-", "-")
	assert.NotEqual(t, first, changed2.fingerprint(), "missing file must change fingerprint")
}

// plan file roundtrip --------------------------------------------------------

func TestPlanSaveLoadRoundtrip(t *testing.T) {
	p := &planFile{
		Version:  planVersion,
		Created:  time.Now().UTC().Truncate(time.Second),
		Session:  "sess",
		Path1:    "/path1/",
		Path2:    "/path2/",
		Workdir:  t.TempDir(),
		BasePath: filepath.Join(t.TempDir(), "base"),
		Resync:   false,
		Changes:  true,
		Config: planConfig{
			CheckFilename: "RCLONE_TEST",
			MaxDelete:     50,
			ConflictLoser: "num",
		},
		Prior:   planListings{Path1: "p1", Path2: "p2"},
		Current: planListings{Path1: "c1", Path2: "c2"},
		Queues: planQueues{
			Copy1to2:      []string{"a", "c"},
			Copy2to1:      []string{"b"},
			Delete1:       nil,
			Delete2:       []string{"a"},
			RenameSkipped: nil,
			SkippedDirs1:  []string{"d"},
		},
		Renames: []planRename{
			{Path1Old: "x", Path1New: "x.conflict1", Path2Old: "x", Path2New: "x.conflict2"},
		},
	}
	path := filepath.Join(t.TempDir(), "nested", "plan.json")
	require.NoError(t, p.save(path))

	loaded, err := loadPlan(path)
	require.NoError(t, err)
	assert.Equal(t, p, loaded)

	// corrupted file rejected
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0600))
	_, err = loadPlan(path)
	assert.Error(t, err)

	// wrong version rejected
	p.Version = planVersion + 1
	require.NoError(t, p.save(path))
	_, err = loadPlan(path)
	assert.Error(t, err)
}

func TestConsumePlanFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	require.NoError(t, os.WriteFile(path, []byte("{}"), 0600))
	dest, err := consumePlanFile(path)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(dest, path+planAppliedSuffix+"-"))
	assert.NoFileExists(t, path, "original plan must be gone after consumption")
	assert.FileExists(t, dest, "plan must be renamed aside atomically")

	// consuming again fails as the plan no longer exists
	_, err = consumePlanFile(path)
	assert.Error(t, err)
}

func fileListWith(names ...string) *fileList {
	ls := newFileList()
	for _, n := range names {
		ls.put(n, 0, time.Time{}, "", "-", "d")
	}
	return ls
}

func TestPlanQueuesStableOrder(t *testing.T) {
	q := queues{
		copy1to2:      bilib.ToNames([]string{"z", "a", "m"}),
		copy2to1:      bilib.ToNames([]string{"b"}),
		delete1:       bilib.ToNames([]string{"z"}),
		delete2:       bilib.Names{},
		renameSkipped: bilib.ToNames([]string{"e", "a"}),
		deletedonboth: bilib.ToNames([]string{"d"}),
		skippedDirs1:  fileListWith("z", "a"),
		skippedDirs2:  newFileList(),
	}
	pq := planQueuesFromRuntime(q)
	assert.Equal(t, []string{"a", "m", "z"}, pq.Copy1to2)
	assert.Equal(t, []string{"a", "e"}, pq.RenameSkipped)
	assert.Equal(t, []string{"a", "z"}, pq.SkippedDirs1)
	assert.Empty(t, pq.Delete2)
	assert.True(t, pq.empty() == false)
}

func TestPlanConfigDiff(t *testing.T) {
	want := planConfig{MaxDelete: 50, CheckFilename: "RCLONE_TEST"}
	got := want
	assert.Empty(t, planConfigDiff(want, got))

	got.MaxDelete = 90
	got.CheckSync = "false"
	diffs := planConfigDiff(want, got)
	keys := []string{}
	for _, d := range diffs {
		keys = append(keys, d.key)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{"checkSync", "maxDelete"}, keys)
}

func TestValidatePlanListings(t *testing.T) {
	want := planListings{Path1: "a", Path2: "b"}
	assert.NoError(t, validatePlanListings("current", want, want))
	err := validatePlanListings("current", want, planListings{Path1: "a", Path2: "CHANGED"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Path2 current listing changed")
}

// integration (local backend) -------------------------------------------------

type planTestEnv struct {
	ctx            context.Context
	dir1, dir2, wd string
	planPath       string
}

func newPlanTestEnv(t *testing.T) *planTestEnv {
	t.Helper()
	root := t.TempDir()
	e := &planTestEnv{
		dir1: filepath.Join(root, "path1"),
		dir2: filepath.Join(root, "path2"),
		wd:   filepath.Join(root, "workdir"),
	}
	require.NoError(t, os.MkdirAll(e.dir1, 0755))
	require.NoError(t, os.MkdirAll(e.dir2, 0755))
	require.NoError(t, os.MkdirAll(e.wd, 0755))
	e.planPath = filepath.Join(e.wd, "test-plan.json")

	ctx, ci := fs.AddConfig(context.Background())
	ci.LogLevel = fs.LogLevelError
	e.ctx = ctx
	return e
}

func (e *planTestEnv) baseOpts() *Options {
	return &Options{
		Workdir:       e.wd,
		MaxDelete:     DefaultMaxDelete,
		CheckFilename: DefaultCheckFilename,
		CheckSync:     CheckSyncTrue,
		NoCleanup:     true,
	}
}

func (e *planTestEnv) run(mutate func(*Options)) error {
	fs1, err := fs.NewFs(e.ctx, e.dir1)
	if err != nil {
		return err
	}
	fs2, err := fs.NewFs(e.ctx, e.dir2)
	if err != nil {
		return err
	}
	opt := e.baseOpts()
	if mutate != nil {
		mutate(opt)
	}
	return Bisync(e.ctx, fs1, fs2, opt)
}

var planBaseTime = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

func writePlanFile(t *testing.T, dir, name, content string, modTime time.Time) {
	t.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	require.NoError(t, os.Chtimes(path, modTime, modTime))
}

func removePlanFile(t *testing.T, dir, name string) {
	t.Helper()
	require.NoError(t, os.Remove(filepath.Join(dir, filepath.FromSlash(name))))
}

func readPlanFile(t *testing.T, dir, name string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
	require.NoError(t, err, "reading %s", name)
	return string(b)
}

func planFileExists(dir, name string) bool {
	_, err := os.Stat(filepath.Join(dir, filepath.FromSlash(name)))
	return !os.IsNotExist(err)
}

func TestBisyncPlanApplyIntegration(t *testing.T) {
	e := newPlanTestEnv(t)

	// identical starting trees
	writePlanFile(t, e.dir1, "file1.txt", "one", planBaseTime)
	writePlanFile(t, e.dir2, "file1.txt", "one", planBaseTime)
	writePlanFile(t, e.dir1, "file2.txt", "two", planBaseTime)
	writePlanFile(t, e.dir2, "file2.txt", "two", planBaseTime)
	writePlanFile(t, e.dir1, "stable.txt", "stable", planBaseTime)
	writePlanFile(t, e.dir2, "stable.txt", "stable", planBaseTime)

	// establish bisync state
	require.NoError(t, e.run(func(o *Options) { o.Resync = true }))
	require.NoError(t, e.run(nil), "settling sync should succeed")

	// Path1: add, modify and delete. Path2: an independent addition.
	now := time.Now().Add(-time.Hour).Truncate(time.Second)
	writePlanFile(t, e.dir1, "file3.txt", "three-new", now)
	writePlanFile(t, e.dir1, "file2.txt", "two-modified", now)
	removePlanFile(t, e.dir1, "file1.txt")
	writePlanFile(t, e.dir2, "file4.txt", "four-new", now)

	// --plan: plan written, nothing changes anywhere
	require.NoError(t, e.run(func(o *Options) { o.Plan = e.planPath }))
	require.FileExists(t, e.planPath)
	p, err := loadPlan(e.planPath)
	require.NoError(t, err)
	assert.True(t, p.Changes)
	// deletions ride along with the copy queue via the --files-from sync
	// semantics, so file1.txt is in copy1to2 as well as delete2
	assert.Equal(t, []string{"file1.txt", "file2.txt", "file3.txt"}, p.Queues.Copy1to2)
	assert.Equal(t, []string{"file4.txt"}, p.Queues.Copy2to1)
	assert.Equal(t, []string{"file1.txt"}, p.Queues.Delete2)
	assert.Empty(t, p.Queues.Delete1)
	assert.Empty(t, p.Renames)
	assert.NotEmpty(t, p.Prior.Path1)
	assert.NotEmpty(t, p.Prior.Path2)
	assert.NotEmpty(t, p.Current.Path1)
	assert.NotEmpty(t, p.Current.Path2)

	// plan run was read-only: Path2 did not receive/delete anything
	assert.False(t, planFileExists(e.dir2, "file3.txt"), "plan must not copy")
	assert.Equal(t, "two", readPlanFile(t, e.dir2, "file2.txt"), "plan must not modify")
	assert.True(t, planFileExists(e.dir2, "file1.txt"), "plan must not delete")

	// static validation failure (different option) must not consume or execute
	err = e.run(func(o *Options) { o.Apply = e.planPath; o.CheckSync = CheckSyncFalse })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "plan validation failed")
	assert.FileExists(t, e.planPath, "plan must not be consumed on validation failure")
	assert.True(t, planFileExists(e.dir2, "file1.txt"), "no operation on validation failure")
	assert.False(t, planFileExists(e.dir2, "file3.txt"), "no operation on validation failure")

	// remote change after planning: a brand new file on Path2
	writePlanFile(t, e.dir2, "file5.txt", "late-change", now)
	err = e.run(func(o *Options) { o.Apply = e.planPath })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "current listing changed")
	assert.FileExists(t, e.planPath, "plan must not be consumed when the remote changed")
	assert.True(t, planFileExists(e.dir2, "file1.txt"), "stale plan must not delete anything")
	assert.False(t, planFileExists(e.dir2, "file3.txt"), "stale plan must not copy anything")
	assert.True(t, planFileExists(e.dir1, "file5.txt") == false, "stale plan must not copy the other way either")

	// undo the late change: state matches the approved plan again
	removePlanFile(t, e.dir2, "file5.txt")
	require.NoError(t, e.run(func(o *Options) { o.Apply = e.planPath }))
	assert.NoFileExists(t, e.planPath, "plan consumed")
	consumed, err := filepath.Glob(e.planPath + planAppliedSuffix + "-*")
	require.NoError(t, err)
	assert.Len(t, consumed, 1, "plan must be renamed aside on consumption")

	// approved changes were applied
	assert.Equal(t, "two-modified", readPlanFile(t, e.dir2, "file2.txt"))
	assert.Equal(t, "three-new", readPlanFile(t, e.dir2, "file3.txt"))
	assert.False(t, planFileExists(e.dir2, "file1.txt"), "delete propagated to Path2")
	assert.Equal(t, "four-new", readPlanFile(t, e.dir1, "file4.txt"), "copy propagated to Path1")

	// the synced state passes a normal follow-up run
	require.NoError(t, e.run(nil))
}

func TestBisyncPlanApplyEmpty(t *testing.T) {
	e := newPlanTestEnv(t)
	writePlanFile(t, e.dir1, "file1.txt", "one", planBaseTime)
	writePlanFile(t, e.dir2, "file1.txt", "one", planBaseTime)
	require.NoError(t, e.run(func(o *Options) { o.Resync = true }))

	require.NoError(t, e.run(func(o *Options) { o.Plan = e.planPath }))
	p, err := loadPlan(e.planPath)
	require.NoError(t, err)
	assert.False(t, p.Changes)
	assert.True(t, p.empty())

	require.NoError(t, e.run(func(o *Options) { o.Apply = e.planPath }))
	assert.NoFileExists(t, e.planPath)
}

func TestBisyncPlanApplyResync(t *testing.T) {
	e := newPlanTestEnv(t)
	writePlanFile(t, e.dir1, "only1.txt", "from-path1", planBaseTime)
	writePlanFile(t, e.dir2, "only2.txt", "from-path2", planBaseTime)

	// plan a resync: nothing is copied yet
	require.NoError(t, e.run(func(o *Options) {
		o.Resync = true
		o.Plan = e.planPath
	}))
	require.FileExists(t, e.planPath)
	p, err := loadPlan(e.planPath)
	require.NoError(t, err)
	assert.True(t, p.Resync)
	// 2to1 pass carries the file unique to Path2, 1to2 the Path1 file
	assert.Contains(t, p.Queues.Copy2to1, "only2.txt")
	assert.Contains(t, p.Queues.Copy1to2, "only1.txt")
	assert.False(t, planFileExists(e.dir1, "only2.txt"), "resync plan must be read-only")
	assert.False(t, planFileExists(e.dir2, "only1.txt"), "resync plan must be read-only")

	// apply performs the resync
	require.NoError(t, e.run(func(o *Options) {
		o.Resync = true
		o.Apply = e.planPath
	}))
	assert.Equal(t, "from-path1", readPlanFile(t, e.dir2, "only1.txt"))
	assert.Equal(t, "from-path2", readPlanFile(t, e.dir1, "only2.txt"))
	assert.NoFileExists(t, e.planPath)

	// follow-up sync is clean
	require.NoError(t, e.run(nil))
}

func TestBisyncPlanApplyConflictRenames(t *testing.T) {
	e := newPlanTestEnv(t)
	writePlanFile(t, e.dir1, "fileX.txt", "same", planBaseTime)
	writePlanFile(t, e.dir2, "fileX.txt", "same", planBaseTime)
	// an unchanged file so the all-files-changed safety check does not trip
	writePlanFile(t, e.dir1, "stable.txt", "stable", planBaseTime)
	writePlanFile(t, e.dir2, "stable.txt", "stable", planBaseTime)
	require.NoError(t, e.run(func(o *Options) { o.Resync = true }))
	require.NoError(t, e.run(nil))

	// both sides change the same file differently: unresolvable conflict,
	// with distinct suffixes per side
	now := time.Now().Add(-time.Hour).Truncate(time.Second)
	writePlanFile(t, e.dir1, "fileX.txt", "path1-version", now)
	writePlanFile(t, e.dir2, "fileX.txt", "path2-version", now.Add(time.Second))

	require.NoError(t, e.run(func(o *Options) {
		o.Plan = e.planPath
		o.ConflictSuffixFlag = "aaa,bbb"
	}))
	p, err := loadPlan(e.planPath)
	require.NoError(t, err)
	require.Len(t, p.Renames, 1, "one conflict decision expected")
	r := p.Renames[0]
	assert.Equal(t, "fileX.txt", r.Path1Old)
	assert.Equal(t, "fileX.txt.aaa1", r.Path1New)
	assert.Equal(t, "fileX.txt", r.Path2Old)
	assert.Equal(t, "fileX.txt.bbb1", r.Path2New)
	// renames not performed during planning
	assert.True(t, planFileExists(e.dir1, "fileX.txt"))
	assert.True(t, planFileExists(e.dir2, "fileX.txt"))
	assert.False(t, planFileExists(e.dir1, "fileX.aaa1"))

	// apply replays the renames and cross-copies the losers
	require.NoError(t, e.run(func(o *Options) {
		o.Apply = e.planPath
		o.ConflictSuffixFlag = "aaa,bbb"
	}))
	assert.Equal(t, "path1-version", readPlanFile(t, e.dir1, "fileX.txt.aaa1"))
	assert.Equal(t, "path2-version", readPlanFile(t, e.dir1, "fileX.txt.bbb1"))
	assert.Equal(t, "path1-version", readPlanFile(t, e.dir2, "fileX.txt.aaa1"))
	assert.Equal(t, "path2-version", readPlanFile(t, e.dir2, "fileX.txt.bbb1"))
	assert.NoFileExists(t, e.planPath)

	require.NoError(t, e.run(nil))
}

func TestBisyncPlanApplyModeValidation(t *testing.T) {
	e := newPlanTestEnv(t)
	writePlanFile(t, e.dir1, "f.txt", "x", planBaseTime)
	writePlanFile(t, e.dir2, "f.txt", "x", planBaseTime)
	require.NoError(t, e.run(func(o *Options) { o.Resync = true }))

	// --plan and --apply are mutually exclusive
	err := e.run(func(o *Options) {
		o.Plan = e.planPath
		o.Apply = e.planPath
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mutually exclusive")

	// --apply + --dry-run rejected (handled by Options.applyContext on the
	// CLI; emulate the same flag combination through the run options)
	o := e.baseOpts()
	o.Apply = e.planPath
	o.DryRun = true
	fs1, err := fs.NewFs(e.ctx, e.dir1)
	require.NoError(t, err)
	fs2, err := fs.NewFs(e.ctx, e.dir2)
	require.NoError(t, err)
	err = Bisync(e.ctx, fs1, fs2, o)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--apply cannot be combined with --dry-run")
}
