# Next Session Plan

## Current State

The project is structurally finished.

What remains is mostly:

1. validation evidence
2. final promotion decisions
3. a few last sandbox UX improvements
4. one environment issue outside the app itself

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

## Environment Issue Still Open

The app is fine, but the machine still has one environment problem:

- full pytest can fail when SciPy loads on Windows because of pagefile / DLL pressure

Observed issue:

- `ImportError: DLL load failed while importing _ufuncs_cxx`
- message from Windows indicates pagefile is too small

This is not a sandbox bug.

## Recommended Next Order

### First

Run fresh evidence again when providers allow:

1. `live_validation_suite`
2. fresh `selected vs guarded` compare
3. one or more live sandbox sessions on:
   - BTC 24/7
   - core equities during active market hours

### Second

If fresh evidence is still weak:

1. use more `replay_as_of_timestamp`
2. use `newsapi` delayed windows to strengthen controlled evidence
3. keep `alphavantage` for fresher windows only

### Third

Only after enough fresh evidence:

1. decide whether `guarded` becomes the selected default
2. update docs and final status accordingly

## Practical Checklist For The Next Session

1. restart Streamlit clean
2. verify the latest sandbox UI behavior on a fresh run
3. run a fresh validation window if provider quota allows
4. run one fresh `selected vs guarded` probe compare
5. run one BTC live sandbox session
6. if the market is open, run one equities live sandbox session
7. save the evidence paths
8. only then revisit final guarded promotion

## Summary

What still matters:

1. more fresh evidence
2. final guarded promotion decision
3. more live sandbox sessions with useful signal
4. minor live-tracking UX polish
5. optional Windows/SciPy environment cleanup
