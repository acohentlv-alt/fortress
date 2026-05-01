# E4 — Status as of 2026-04-30 (deferred)

## What was built

E4 (drill-down + count fix) was implemented by an executor agent. The work
exists as a single commit:

- **Commit SHA**: `83552b7` (also tagged `e4-wip-2026-04-30`)
- **Worktree path** (still on disk): `.claude/worktrees/agent-af9adb5beb814da51`
- **Worktree branch**: `worktree-agent-af9adb5beb814da51`

## Why it's not on main

The Agent tool with `isolation: "worktree"` forked the executor's worktree
from the WRONG base — `451e735` (Alan's `time_cap_total_min` commit) instead
of `e8ccabc` (E1+E2+E3, which was the actual main HEAD at launch time).

Result: E4's edits are layered on a base that does NOT have E1+E2+E3
features. The diff is internally consistent but rebasing onto `e8ccabc`
produces 5 conflict markers in `queries_panel.js` (intersection of E1's
3-state rendering [done / running / queued] and E4's done-row modifications
[clickable + count fix]). Those conflicts need real design thought to
integrate cleanly — not just textual resolution.

## To pick up tomorrow

### Option A — Plan a merge-resolution brief (~30-45 min)

Send a focused planner brief: "Here's E4 commit `83552b7`. Here's main HEAD
`e8ccabc`. Integrate E4's drill-down + count fix into E1's 3-state queries
panel cleanly." Planner outputs a brief; reviewer verifies; executor implements.

### Option B — Discard E4, re-/exec from clean base (~10-30 min)

Diff the E4 commit's changes (`git show 83552b7`), use them as reference,
re-run /exec for E4 with the brief at `docs/wip/E4_BRIEF.md`. Hope the new
worktree forks from main HEAD correctly this time (the previous fork was
likely a one-time glitch).

### Option C — Manual integration (~30 min)

Read both branches' code in `queries_panel.js`, write the integrated done-row
rendering by hand. Risk of subtle bugs the QA misses.

## Files involved

E4 modified 9 files (per executor's diff stat at the wrong base):
- `fortress/api/routes/jobs.py` (+21 / -7)
- `fortress/api/routes/export.py` (+45 / -7)
- `fortress/frontend/css/components.css` (+16 / 0)
- `fortress/frontend/js/api.js` (+10 / -2)
- `fortress/frontend/js/components/queries_panel.js` (+123 / -32) ← biggest
- `fortress/frontend/js/pages/job.js` (+81 / -10)
- `fortress/frontend/js/pages/monitor.js` (+9 / -2)
- `fortress/frontend/translations/fr.json` (+6 / 0)
- `fortress/frontend/translations/en.json` (+6 / 0)

Total: 260 insertions, 57 deletions.

## QA also pending

When E4 ships, run E4's QA test plan from `docs/wip/E4_BRIEF.md` BLOCK 3.
Plus the deferred E2 + E3 visual tests for E1+E2+E3 — those were never
fully visual-verified before E1+E2+E3 was merged on 2026-04-30 evening.
