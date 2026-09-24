package bisync

import (
	"context"
	"fmt"
	"os"
	"slices"

	"github.com/rclone/rclone/cmd/bisync/bilib"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/lib/terminal"
)

// applyRun validates every part of an approved plan against the current
// state and only starts executing once all checks passed. The plan file is
// atomically consumed before the first mutation, so it cannot be applied a
// second time.
func (b *bisyncRun) applyRun(octx, fctx context.Context) (err error) {
	opt := b.opt
	path1 := bilib.FsPath(b.fs1)
	path2 := bilib.FsPath(b.fs2)
	fs.Infof(nil, "Validating and applying bisync plan %s for Path1 %s with Path2 %s", opt.Apply, quotePath(path1), quotePath(path2))

	plan, err := loadPlan(opt.Apply)
	if err != nil {
		// bad plan file is a user error, not a sync failure: return it as-is
		return err
	}
	b.applyPlan = plan

	// If validation fails after marching, discard the fresh new-listings so
	// the next run starts clean. A successful run cleans them normally.
	committed := false
	defer func() {
		if !committed && !b.opt.NoCleanup {
			_ = os.Remove(b.newListing1)
			_ = os.Remove(b.newListing2)
		}
	}()

	// 1. Static validation: paths, workdir, configuration and filters.
	// Validation refusals must not touch any state, so they are returned
	// plain rather than marked critical.
	if err = b.validateStatic(fctx, plan); err != nil {
		return err
	}

	// 2. Fresh listing of both paths (read-only) and fingerprint comparison
	fs.Infof(nil, "Building Path1 and Path2 listings")
	ls1, ls2, err := b.makeMarchListing(fctx)
	if err != nil || accounting.Stats(fctx).Errored() {
		fs.Error(nil, Color(terminal.RedFg, "There were errors while building listings. Aborting as it is too dangerous to continue."))
		b.critical = true
		b.retryable = true
		return err
	}
	current := planListings{Path1: ls1.fingerprint(), Path2: ls2.fingerprint()}
	if err = validatePlanListings("current", plan.Current, current); err != nil {
		return err
	}

	if plan.Resync {
		return b.applyResyncPlan(fctx, plan)
	}

	// 3. Previous successful listings must still be the ones the plan was
	// based on (--recover may revert to the -old backups)
	if err = b.ensurePriorListings(bilib.FsPath(b.fs1), bilib.FsPath(b.fs2)); err != nil {
		return err
	}
	prior1, err := b.listingFingerprint(b.listing1)
	if err != nil {
		return fmt.Errorf("failed to fingerprint prior Path1 listing: %w", err)
	}
	prior2, err := b.listingFingerprint(b.listing2)
	if err != nil {
		return fmt.Errorf("failed to fingerprint prior Path2 listing: %w", err)
	}
	if err = validatePlanListings("prior", plan.Prior, planListings{Path1: prior1, Path2: prior2}); err != nil {
		return err
	}

	// 4. Recompute the read-only deltas and the decisions they imply, and
	// prove the approved plan still decides exactly the same operations
	fs.Infof(nil, "Path1 checking for diffs")
	ds1, err := b.findDeltas(fctx, b.fs1, b.listing1, ls1, "Path1")
	if err != nil {
		return err
	}
	fs.Infof(nil, "Path2 checking for diffs")
	ds2, err := b.findDeltas(fctx, b.fs2, b.listing2, ls2, "Path2")
	if err != nil {
		return err
	}

	// Run the decision engine in dry-run mode so conflict renames and loser
	// deletes are computed but not performed.
	wasDryRun := b.opt.DryRun
	wasCapture := b.planCapture
	b.opt.DryRun = true
	b.planCapture = true
	decided, dirs1, dirs2, err := b.decideDeltas(octx, ds1, ds2)
	// restore real execution mode BEFORE consuming the plan and executing
	b.opt.DryRun = wasDryRun
	b.planCapture = wasCapture
	if err != nil {
		b.critical = true
		return err
	}
	if err = b.validatePlanDecisions(plan, decided); err != nil {
		return err
	}

	// ALL VALIDATION CHECKS PASSED. Consume the plan atomically before any
	// mutation, so a second --apply (or a crash) cannot execute it again.
	consumed, err := consumePlanFile(opt.Apply)
	if err != nil {
		return err
	}
	fs.Infof(nil, Color(terminal.GreenFg, "Plan validated successfully; consumed %s and applying the approved changes"), consumed)
	committed = true

	// Execute the queues saved in the approved plan (not the recomputed
	// ones), restoring full directory info from the fresh listings.
	q := plan.Queues.toRuntime()
	q.delete1, q.delete2 = plan.Queues.deleteQueues()
	q.skippedDirs1 = subFileList(dirs1, plan.Queues.SkippedDirs1)
	q.skippedDirs2 = subFileList(dirs2, plan.Queues.SkippedDirs2)
	b.renames = plan.toRuntimeRenames()

	if err = b.replayPlanRenames(octx, plan); err != nil {
		b.critical = true
		return err
	}

	results2to1, results1to2, err := b.executeQueues(octx, q, dirs1, dirs2)
	if err != nil {
		b.critical = true
		return err
	}

	return b.finalizeApply(fctx, plan, results2to1, results1to2, q)
}

// applyResyncPlan consumes the validated plan and performs the real resync,
// asserting afterwards that the executed copy sets equal the approved ones.
func (b *bisyncRun) applyResyncPlan(fctx context.Context, plan *planFile) error {
	consumed, err := consumePlanFile(b.opt.Apply)
	if err != nil {
		b.critical = true
		return err
	}
	fs.Infof(nil, Color(terminal.GreenFg, "Resync plan validated successfully; consumed %s and applying the approved changes"), consumed)
	return b.resync(fctx)
}

// validatePlanDecisions proves that the queues and rename/delete decisions
// implied by the current state are exactly the approved ones.
func (b *bisyncRun) validatePlanDecisions(p *planFile, q queues) error {
	got := planQueuesFromRuntime(q)
	var problems []string
	addSet := func(label string, want, got []string) {
		if !slices.Equal(want, got) {
			problems = append(problems, fmt.Sprintf("%s queue differs from the approved plan (plan %v, current %v)", label, want, got))
		}
	}
	addSet("copy1to2", p.Queues.Copy1to2, got.Copy1to2)
	addSet("copy2to1", p.Queues.Copy2to1, got.Copy2to1)
	addSet("delete1", p.Queues.Delete1, got.Delete1)
	addSet("delete2", p.Queues.Delete2, got.Delete2)
	addSet("renameSkipped", p.Queues.RenameSkipped, got.RenameSkipped)
	addSet("deletedonboth", p.Queues.DeletedOnBoth, got.DeletedOnBoth)
	addSet("skippedDirs1", p.Queues.SkippedDirs1, got.SkippedDirs1)
	addSet("skippedDirs2", p.Queues.SkippedDirs2, got.SkippedDirs2)

	gotRenames := planRenamesFromRuntime(b.renames)
	if !slices.EqualFunc(p.Renames, gotRenames, func(a, b planRename) bool { return a == b }) {
		problems = append(problems, fmt.Sprintf("conflict decisions differ from the approved plan (plan %v, current %v)", p.Renames, gotRenames))
	}

	if len(problems) > 0 {
		return fmt.Errorf(Color(terminal.RedFg, "plan validation failed, no changes were made:\n  - %s"), joinProblems(problems))
	}
	return nil
}

func joinProblems(problems []string) string {
	out := ""
	for i, p := range problems {
		if i > 0 {
			out += "\n  - "
		}
		out += p
	}
	return out
}

// subFileList returns a new fileList containing only the entries named,
// copying their full info from src.
func subFileList(src *fileList, names []string) *fileList {
	dst := newFileList()
	if src == nil {
		return dst
	}
	for _, name := range names {
		if src.has(name) {
			src.getPut(name, dst)
		}
	}
	return dst
}

// replayPlanRenames performs the approved conflict renames and loser
// deletes, in the stable order stored in the plan.
func (b *bisyncRun) replayPlanRenames(ctx context.Context, p *planFile) error {
	for _, r := range p.Renames {
		switch {
		case r.Path1New == "":
			fs.Infof(nil, "Applying approved decision: deleting %s from Path1", r.Path1Old)
			if err := b.deleteConflictFile(ctx, 1, r.Path1Old); err != nil {
				return err
			}
		case r.Path1New != r.Path1Old:
			fs.Infof(nil, "Applying approved decision: renaming Path1 %s to %s", r.Path1Old, r.Path1New)
			if err := b.moveConflictFile(ctx, 1, r.Path1Old, r.Path1New); err != nil {
				return err
			}
		}
		switch {
		case r.Path2New == "":
			fs.Infof(nil, "Applying approved decision: deleting %s from Path2", r.Path2Old)
			if err := b.deleteConflictFile(ctx, 2, r.Path2Old); err != nil {
				return err
			}
		case r.Path2New != r.Path2Old:
			fs.Infof(nil, "Applying approved decision: renaming Path2 %s to %s", r.Path2Old, r.Path2New)
			if err := b.moveConflictFile(ctx, 2, r.Path2Old, r.Path2New); err != nil {
				return err
			}
		}
	}
	return nil
}

// finalizeApply updates the listings from the executed results, removes new
// listings, runs the post-sync listing validation and optional empty
// directory cleanup.
func (b *bisyncRun) finalizeApply(fctx context.Context, p *planFile, results2to1, results1to2 []Results, q queues) error {
	fs.Infof(nil, "Updating listings")
	b.saveOldListings()
	if !p.Changes {
		b.replaceCurrentListings()
	} else {
		err1 := b.modifyListing(fctx, b.fs2, b.fs1, results2to1, q, false) // 2to1
		err2 := b.modifyListing(fctx, b.fs1, b.fs2, results1to2, q, true)  // 1to2
		if err1 != nil {
			b.critical = true
			b.retryable = true
			return err1
		}
		if err2 != nil {
			b.critical = true
			b.retryable = true
			return err2
		}
	}

	return b.postSyncChecks(fctx)
}

// resyncResultsNames converts resync sync results to the transferred name set
func (b *bisyncRun) resyncResultsNames(results []Results) bilib.Names {
	names := bilib.Names{}
	for _, result := range results {
		if result.Name != "" &&
			(result.Flags != "d" || b.opt.CreateEmptySrcDirs) &&
			result.IsSrc && result.Src != "" &&
			(result.Winner.Err == nil || result.Flags == "d") {
			names.Add(result.Name)
		}
	}
	return names
}

// validateResyncResults checks that every file named by the approved plan
// was actually transferred. The real run may log additional (no-op) entries
// compared with the dry-run plan, because the first resync pass can make
// files already-equal for the second pass, so only a subset check is used;
// the final checkSync validation proves both paths converged.
func (b *bisyncRun) validateResyncResults(p *planFile, results2to1, results1to2 []Results) error {
	var problems []string
	checkSet := func(label string, want []string, got bilib.Names) {
		var missing []string
		for _, name := range want {
			if !got.Has(name) {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			problems = append(problems, fmt.Sprintf("%s approved resync transfers were not executed: %v", label, missing))
		}
	}
	checkSet("copy2to1", p.Queues.Copy2to1, b.resyncResultsNames(results2to1))
	checkSet("copy1to2", p.Queues.Copy1to2, b.resyncResultsNames(results1to2))
	if len(problems) > 0 {
		return fmt.Errorf(Color(terminal.RedFg, "applied resync did not satisfy the approved plan:\n  - %s"), joinProblems(problems))
	}
	return nil
}
