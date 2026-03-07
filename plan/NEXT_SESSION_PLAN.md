# Next Session Plan

## Current State

The project is structurally finished.

What remains is mostly:

1. validation evidence
2. final promotion decisions
3. a few last sandbox UX improvements
4. one last sandbox instrumentation gap
5. one environment issue outside the app itself

## What Still Matters Most

### 1. Guarded map promotion is still open

Current truth:

- the `guarded` lineage improved several archived-live and fresh probe comparisons
- but it still does **not** beat the pure baseline in grouped aggregate research

So:

- do **not** promote `guarded` as the final default lineage yet
- keep it as the leading candidate under evaluation

### 2. More fresh evidence is still needed

This is still the main blocker.

Need:

- more `fresh_sync` windows
- more supported-event volume in those fresh windows
- more fresh probe compares for `selected vs guarded`

Goal:

- decide promotion using repeated fresh evidence, not one good batch

### 3. More live sandbox sessions are still needed

The engine works.

What is still missing:

- more sessions with actually actionable live signal
- more sessions with price movement that is not stale
- more observation of when the event layer truly enters risk

This is validation, not missing architecture.

### 4. Immediate sandbox instrumentation gap

#### Explicit useless-news breakdown in the sandbox UI

Current truth:

- the UI already shows:
  - `articles_seen`
  - `inserted`
  - `events`
  - provider and refresh status
- but it still does **not** explicitly tell whether the news was:
  - weak
  - filtered
  - commentary
  - recap
  - non-eligible
  - actually usable

So next session should add a sandbox news-quality block with:

1. `articles_seen`
2. `inserted`
3. `events_processed`
4. `watchlist_eligible`
5. `filtered_low_quality`
6. `filtered_commentary_or_recap`
7. `usable_signal = yes/no`

## Sandbox UX Items Still Worth Doing

These are smaller than the evidence gap, but still useful:

1. show explicit `ETA` in the live tracking block
2. show a stronger badge for:
   - `market closed`
   - `price feed stale`
   - `price feed advancing`
3. color the refresh status:
   - success
   - skipped
   - quota cooldown
   - error
4. optionally show:
   - `last refresh at`
   - `next expected step`
5. de-overlap identical capital lines or show a current-value table beside the chart
6. keep the current run visible without loading stale historical runs by accident

## Environment Issue Still Open

The app is fine, but the machine still has one environment problem:

- full pytest can fail when SciPy loads on Windows because of pagefile / DLL pressure

Observed issue:

- `ImportError: DLL load failed while importing _ufuncs_cxx`
- message from Windows indicates pagefile is too small

This is not a sandbox bug.

## Recommended Next Order

### First

Add the missing sandbox news-quality breakdown:

1. explicit useless-news / filtered-news breakdown

### Second

Run fresh evidence again when providers allow:

1. `live_validation_suite`
2. fresh `selected vs guarded` compare
3. one or more live sandbox sessions on:
   - BTC 24/7
   - core equities during active market hours

### Third

If fresh evidence is still weak:

1. use more `replay_as_of_timestamp`
2. use `newsapi` delayed windows to strengthen controlled evidence
3. keep `alphavantage` for fresher windows only

### Fourth

Only after enough fresh evidence:

1. decide whether `guarded` becomes the selected default
2. update docs and final status accordingly

## Practical Checklist For The Next Session

1. restart Streamlit clean
2. add explicit useless-news / filtered-news breakdown in the sandbox UI
3. verify the latest sandbox UI behavior on a fresh run
4. run a fresh validation window if provider quota allows
5. run one fresh `selected vs guarded` probe compare
6. run one BTC live sandbox session
7. if the market is open, run one equities live sandbox session
8. save the evidence paths
9. only then revisit final guarded promotion

## Summary

What still matters:

1. more fresh evidence
2. final guarded promotion decision
3. more live sandbox sessions with useful signal
4. make useless-news / filtered-news explicit in the sandbox UI
5. minor live-tracking UX polish
6. optional Windows/SciPy environment cleanup
