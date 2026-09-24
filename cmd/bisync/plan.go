package bisync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/rclone/rclone/cmd/bisync/bilib"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/terminal"
)

// planVersion is the version of the plan file format
const planVersion = 1

// planAppliedSuffix is appended to a plan file when it is consumed
const planAppliedSuffix = ".applied"

// planFile is the serializable plan/apply document.
//
// It is produced by a read-only --plan run after listing and conflict
// resolution, and consumed by a later --apply run which first proves that
// nothing relevant changed since the plan was approved.
type planFile struct {
	Version int `json:"version"`
	// Created is when the plan was written
	Created time.Time `json:"created"`
	// Session identifies the pair of synced paths
	Session string `json:"session"`
	Path1   string `json:"path1"`
	Path2   string `json:"path2"`
	// Workdir and BasePath bind the plan to one bisync working directory
	Workdir  string `json:"workdir"`
	BasePath string `json:"basePath"`
	// Resync is true when the plan was made during a --resync run
	Resync bool `json:"resync"`
	// Changes is false when the run found no deltas
	Changes bool `json:"changes"`
	// Config is the decision-relevant subset of the options and fs config
	Config planConfig `json:"config"`
	// Filters records the filters file and its content hash, if any
	Filters *planFilters `json:"filters,omitempty"`
	// Prior are fingerprints of the previous successful listings
	Prior planListings `json:"priorListings"`
	// Current are fingerprints of the listings captured at plan time
	Current planListings `json:"currentListings"`
	// Queues are the approved, stably ordered file sets
	Queues planQueues `json:"queues"`
	// Renames are the approved conflict renames and loser deletes,
	// sorted stably by Path1Old
	Renames []planRename `json:"renames,omitempty"`
}

// planConfig is the subset of the configuration that affects planning
// decisions. An --apply run must present an identical configuration.
type planConfig struct {
	ResyncMode             string     `json:"resyncMode"`
	CheckAccess            bool       `json:"checkAccess"`
	CheckFilename          string     `json:"checkFilename"`
	CheckSync              string     `json:"checkSync"`
	CreateEmptySrcDirs     bool       `json:"createEmptySrcDirs"`
	RemoveEmptyDirs        bool       `json:"removeEmptyDirs"`
	MaxDelete              int        `json:"maxDelete"`
	Force                  bool       `json:"force"`
	FiltersFile            string     `json:"filtersFile"`
	OrigBackupDir          string     `json:"origBackupDir"`
	BackupDir1             string     `json:"backupDir1"`
	BackupDir2             string     `json:"backupDir2"`
	IgnoreListingChecksum  bool       `json:"ignoreListingChecksum"`
	Resilient              bool       `json:"resilient"`
	Recover                bool       `json:"recover"`
	Compare                CompareOpt `json:"compare"`
	ConflictResolve        string     `json:"conflictResolve"`
	ConflictLoser          string     `json:"conflictLoser"`
	ConflictSuffix1        string     `json:"conflictSuffix1"`
	ConflictSuffix2        string     `json:"conflictSuffix2"`
	NoUnicodeNormalization bool       `json:"noUnicodeNormalization"`
	IgnoreCaseSync         bool       `json:"ignoreCaseSync"`
	FixCase                bool       `json:"fixCase"`
	SizeOnly               bool       `json:"sizeOnly"`
	CheckSum               bool       `json:"checkSum"`
	IgnoreSize             bool       `json:"ignoreSize"`
	SuffixKeepExtension    bool       `json:"suffixKeepExtension"`
}

// planFilters records the filters file path and its md5 hash
type planFilters struct {
	File string `json:"file"`
	MD5  string `json:"md5"`
}

// planListings holds content fingerprints for the Path1 and Path2 listings
type planListings struct {
	Path1 string `json:"path1"`
	Path2 string `json:"path2"`
}

// planQueues holds the approved file name sets in stable (sorted) order
type planQueues struct {
	Copy1to2      []string `json:"copy1to2"`
	Copy2to1      []string `json:"copy2to1"`
	Delete1       []string `json:"delete1"`
	Delete2       []string `json:"delete2"`
	RenameSkipped []string `json:"renameSkipped"`
	DeletedOnBoth []string `json:"deletedonboth"`
	SkippedDirs1  []string `json:"skippedDirs1"`
	SkippedDirs2  []string `json:"skippedDirs2"`
}

// planRename records one conflict decision. An empty NewName means the
// loser copy on that path is deleted.
type planRename struct {
	Path1Old string `json:"path1Old"`
	Path1New string `json:"path1New"`
	Path2Old string `json:"path2Old"`
	Path2New string `json:"path2New"`
}

// empty reports whether the plan contains no changes to execute
func (p *planFile) empty() bool {
	return !p.Changes && len(p.Renames) == 0 && p.Queues.empty()
}

func (q planQueues) empty() bool {
	return len(q.Copy1to2) == 0 && len(q.Copy2to1) == 0 &&
		len(q.Delete1) == 0 && len(q.Delete2) == 0 &&
		len(q.RenameSkipped) == 0 && len(q.DeletedOnBoth) == 0 &&
		len(q.SkippedDirs1) == 0 && len(q.SkippedDirs2) == 0
}

// fingerprint returns a stable sha256 hex hash of the listing entries,
// independent of the timestamped header line.
func (ls *fileList) fingerprint() string {
	ls.sort()
	h := sha256.New()
	hashName := ""
	if ls.hash != hash.None {
		hashName = ls.hash.String()
	}
	for _, remote := range ls.list {
		fi := ls.get(remote)
		hashVal := "-"
		if hashName != "" && fi.hash != "" {
			hashVal = hashName + ":" + fi.hash
		}
		id := fi.id
		if id == "" {
			id = "-"
		}
		flags := fi.flags
		if flags == "" {
			flags = "-"
		}
		fmt.Fprintf(h, lineFormat, flags, fi.size, hashVal, id, fi.time.In(TZ).Format(timeFormat), remote)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// listingFingerprint fingerprints a listing on disk, returning "" when it
// does not exist or cannot be parsed.
func (b *bisyncRun) listingFingerprint(listing string) (string, error) {
	if !bilib.FileExists(listing) {
		return "", nil
	}
	ls, err := b.loadListing(listing)
	if err != nil {
		return "", err
	}
	return ls.fingerprint(), nil
}

// snapshotPlanConfig captures the decision-relevant configuration
func (b *bisyncRun) snapshotPlanConfig(ctx context.Context) planConfig {
	opt := b.opt
	ci := fs.GetConfig(ctx)
	return planConfig{
		ResyncMode:             opt.ResyncMode.String(),
		CheckAccess:            opt.CheckAccess,
		CheckFilename:          opt.CheckFilename,
		CheckSync:              opt.CheckSync.String(),
		CreateEmptySrcDirs:     opt.CreateEmptySrcDirs,
		RemoveEmptyDirs:        opt.RemoveEmptyDirs,
		MaxDelete:              opt.MaxDelete,
		Force:                  opt.Force,
		FiltersFile:            opt.FiltersFile,
		OrigBackupDir:          opt.OrigBackupDir,
		BackupDir1:             opt.BackupDir1,
		BackupDir2:             opt.BackupDir2,
		IgnoreListingChecksum:  opt.IgnoreListingChecksum,
		Resilient:              opt.Resilient,
		Recover:                opt.Recover,
		Compare:                opt.Compare,
		ConflictResolve:        opt.ConflictResolve.String(),
		ConflictLoser:          opt.ConflictLoser.String(),
		ConflictSuffix1:        opt.ConflictSuffix1,
		ConflictSuffix2:        opt.ConflictSuffix2,
		NoUnicodeNormalization: ci.NoUnicodeNormalization,
		IgnoreCaseSync:         ci.IgnoreCaseSync,
		FixCase:                ci.FixCase,
		SizeOnly:               ci.SizeOnly,
		CheckSum:               ci.CheckSum,
		IgnoreSize:             ci.IgnoreSize,
		SuffixKeepExtension:    ci.SuffixKeepExtension,
	}
}

// makePlan builds a planFile from the current run state and decisions
func (b *bisyncRun) makePlan(ctx context.Context, q queues, changes bool) (*planFile, error) {
	opt := b.opt

	prior1, err := b.listingFingerprint(b.listing1)
	if err != nil {
		return nil, fmt.Errorf("failed to fingerprint prior Path1 listing: %w", err)
	}
	prior2, err := b.listingFingerprint(b.listing2)
	if err != nil {
		return nil, fmt.Errorf("failed to fingerprint prior Path2 listing: %w", err)
	}

	current1 := ""
	current2 := ""
	if b.march.ls1 != nil {
		current1 = b.march.ls1.fingerprint()
	}
	if b.march.ls2 != nil {
		current2 = b.march.ls2.fingerprint()
	}

	p := &planFile{
		Version:  planVersion,
		Created:  time.Now().UTC(),
		Session:  bilib.SessionName(b.fs1, b.fs2),
		Path1:    bilib.FsPath(b.fs1),
		Path2:    bilib.FsPath(b.fs2),
		Workdir:  b.workDir,
		BasePath: b.basePath,
		Resync:   opt.Resync,
		Changes:  changes,
		Config:   b.snapshotPlanConfig(ctx),
		Prior: planListings{
			Path1: prior1,
			Path2: prior2,
		},
		Current: planListings{
			Path1: current1,
			Path2: current2,
		},
		Queues: planQueuesFromRuntime(q),
	}
	if opt.FiltersFile != "" {
		p.Filters = &planFilters{
			File: opt.FiltersFile,
			MD5:  opt.filtersFileHash,
		}
	}
	p.Renames = planRenamesFromRuntime(b.renames)
	return p, nil
}

// savePlan builds and atomically writes the plan file
func (b *bisyncRun) savePlan(ctx context.Context, q queues, changes bool) error {
	p, err := b.makePlan(ctx, q, changes)
	if err != nil {
		return err
	}
	if err := p.save(b.opt.Plan); err != nil {
		return fmt.Errorf("failed to write plan file %q: %w", b.opt.Plan, err)
	}
	fs.Infof(nil, Color(terminal.GreenFg, "Plan written to %s (%d copies, %d deletes, %d conflict decisions). No changes were made."),
		b.opt.Plan, len(p.Queues.Copy1to2)+len(p.Queues.Copy2to1),
		len(p.Queues.Delete1)+len(p.Queues.Delete2), len(p.Renames))
	return nil
}

// save writes the plan atomically by staging it next to the destination and
// renaming it into place.
func (p *planFile) save(path string) error {
	data, err := json.MarshalIndent(p, "", "\t")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return err
		}
	}
	tmp, err := os.CreateTemp(dir, ".bisync-plan-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Chmod(tmpName, bilib.PermSecure); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// loadPlan reads and decodes a plan file
func loadPlan(path string) (*planFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read plan file %q: %w", path, err)
	}
	p := &planFile{}
	if err := json.Unmarshal(data, p); err != nil {
		return nil, fmt.Errorf("cannot parse plan file %q: %w", path, err)
	}
	if p.Version != planVersion {
		return nil, fmt.Errorf("unsupported plan file version %d (expected %d): %s", p.Version, planVersion, path)
	}
	return p, nil
}

// consumePlanFile atomically renames the plan file aside so it cannot be
// applied twice. It returns the new path. Must only be called after every
// validation check passed.
func consumePlanFile(path string) (string, error) {
	dest := fmt.Sprintf("%s%s-%s", path, planAppliedSuffix, time.Now().UTC().Format("20060102-150405.000000000"))
	if err := os.Rename(path, dest); err != nil {
		return "", fmt.Errorf("failed to consume plan file %q: %w", path, err)
	}
	return dest, nil
}

// planQueuesFromRuntime converts the runtime queues to stably ordered plan queues
func planQueuesFromRuntime(q queues) planQueues {
	return planQueues{
		Copy1to2:      q.copy1to2.ToList(),
		Copy2to1:      q.copy2to1.ToList(),
		Delete1:       q.delete1.ToList(),
		Delete2:       q.delete2.ToList(),
		RenameSkipped: q.renameSkipped.ToList(),
		DeletedOnBoth: q.deletedonboth.ToList(),
		SkippedDirs1:  sortedNames(q.skippedDirs1),
		SkippedDirs2:  sortedNames(q.skippedDirs2),
	}
}

func sortedNames(ls *fileList) []string {
	if ls == nil {
		return nil
	}
	out := slices.Clone(ls.list)
	sort.Strings(out)
	return out
}

// planRenamesFromRuntime converts the runtime rename decisions to a stably
// ordered plan slice.
func planRenamesFromRuntime(rs renames) []planRename {
	if len(rs) == 0 {
		return nil
	}
	keys := make([]string, 0, len(rs))
	for k := range rs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]planRename, 0, len(keys))
	for _, k := range keys {
		r := rs[k]
		out = append(out, planRename{
			Path1Old: r.path1.oldName,
			Path1New: r.path1.newName,
			Path2Old: r.path2.oldName,
			Path2New: r.path2.newName,
		})
	}
	return out
}

// toRuntime converts plan queues back to runtime queues
func (q planQueues) toRuntime() queues {
	return queues{
		copy1to2:      bilib.ToNames(q.Copy1to2),
		copy2to1:      bilib.ToNames(q.Copy2to1),
		renameSkipped: bilib.ToNames(q.RenameSkipped),
		skippedDirs1:  fileListFromNames(q.SkippedDirs1),
		skippedDirs2:  fileListFromNames(q.SkippedDirs2),
		deletedonboth: bilib.ToNames(q.DeletedOnBoth),
	}
}

// deleteQueues returns the runtime delete queues, which are not needed during
// sync but are recorded for approval and listing updates.
func (q planQueues) deleteQueues() (delete1, delete2 bilib.Names) {
	return bilib.ToNames(q.Delete1), bilib.ToNames(q.Delete2)
}

func fileListFromNames(names []string) *fileList {
	ls := newFileList()
	for _, name := range names {
		ls.list = append(ls.list, name)
		ls.info[name] = &fileInfo{flags: "d"}
	}
	return ls
}

// toRuntimeRenames converts plan renames back to runtime renames
func (p *planFile) toRuntimeRenames() renames {
	if len(p.Renames) == 0 {
		return renames{}
	}
	rs := renames{}
	for _, pr := range p.Renames {
		rs[pr.Path1Old] = renamesInfo{
			path1: namePair{oldName: pr.Path1Old, newName: pr.Path1New},
			path2: namePair{oldName: pr.Path2Old, newName: pr.Path2New},
		}
	}
	return rs
}

// validateStatic checks every part of the plan that does not require a fresh
// listing. It returns a single error describing every mismatch found, and
// performs no changes.
func (b *bisyncRun) validateStatic(ctx context.Context, p *planFile) error {
	var problems []string
	add := func(format string, a ...any) {
		problems = append(problems, fmt.Sprintf(format, a...))
	}

	if p.Version != planVersion {
		add("plan version is %d but this rclone supports %d", p.Version, planVersion)
	}
	if got := bilib.SessionName(b.fs1, b.fs2); got != p.Session {
		add("session mismatch: plan was made for %q, current session is %q", p.Session, got)
	}
	if got := bilib.FsPath(b.fs1); got != p.Path1 {
		add("path1 mismatch: plan was made for %q, current path1 is %q", p.Path1, got)
	}
	if got := bilib.FsPath(b.fs2); got != p.Path2 {
		add("path2 mismatch: plan was made for %q, current path2 is %q", p.Path2, got)
	}
	if b.workDir != p.Workdir {
		add("workdir mismatch: plan was made for %q, current workdir is %q", p.Workdir, b.workDir)
	}
	if b.basePath != p.BasePath {
		add("basePath mismatch: plan was made for %q, current basePath is %q", p.BasePath, b.basePath)
	}
	if b.opt.Resync != p.Resync {
		add("resync mismatch: plan was made with resync=%v, current is resync=%v", p.Resync, b.opt.Resync)
	}

	gotConfig := b.snapshotPlanConfig(ctx)
	for _, diff := range planConfigDiff(p.Config, gotConfig) {
		add("configuration mismatch for %s: plan %s, current %s", diff.key, diff.want, diff.got)
	}

	if p.Filters != nil {
		if b.opt.FiltersFile != p.Filters.File {
			add("filters file mismatch: plan was made with %q, current is %q", p.Filters.File, b.opt.FiltersFile)
		}
		if b.opt.filtersFileHash != p.Filters.MD5 {
			add("filters file content changed since the plan was made (md5 plan %s, current %s)", p.Filters.MD5, b.opt.filtersFileHash)
		}
	} else if b.opt.FiltersFile != "" {
		add("filters file mismatch: plan was made without a filters file, current is %q", b.opt.FiltersFile)
	}

	if len(problems) > 0 {
		return fmt.Errorf(Color(terminal.RedFg, "plan validation failed, no changes were made:\n  - %s"), strings.Join(problems, "\n  - "))
	}
	return nil
}

type configDiff struct {
	key  string
	want string
	got  string
}

// planConfigDiff returns the fields that differ between the planned and the
// current configuration.
func planConfigDiff(want, got planConfig) []configDiff {
	var diffs []configDiff
	wm := mustJSONMap(want)
	gm := mustJSONMap(got)
	keys := map[string]bool{}
	for k := range wm {
		keys[k] = true
	}
	for k := range gm {
		keys[k] = true
	}
	for k := range keys {
		if !reflect.DeepEqual(wm[k], gm[k]) {
			diffs = append(diffs, configDiff{key: k, want: fmt.Sprint(wm[k]), got: fmt.Sprint(gm[k])})
		}
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].key < diffs[j].key })
	return diffs
}

func mustJSONMap(v any) map[string]any {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	m := map[string]any{}
	if err := json.Unmarshal(data, &m); err != nil {
		panic(err)
	}
	return m
}

// validateListings compares planned and current fingerprints for both sides.
// All mismatches are collected before returning so the operator sees every
// reason for the refusal.
func validatePlanListings(what string, want, got planListings) error {
	var problems []string
	if want.Path1 != got.Path1 {
		problems = append(problems, fmt.Sprintf("Path1 %s listing changed since the plan was made (fingerprint plan %s, current %s)", what, want.Path1, got.Path1))
	}
	if want.Path2 != got.Path2 {
		problems = append(problems, fmt.Sprintf("Path2 %s listing changed since the plan was made (fingerprint plan %s, current %s)", what, want.Path2, got.Path2))
	}
	if len(problems) > 0 {
		return fmt.Errorf(Color(terminal.RedFg, "plan validation failed, no changes were made:\n  - %s"), strings.Join(problems, "\n  - "))
	}
	return nil
}
