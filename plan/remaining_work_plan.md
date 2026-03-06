# Remaining Work Plan

This file is a condensed secondary view. The canonical roadmap lives in `plan/master_execution_plan.md`.

## Done

These parts are already implemented:

- quant risk core
- NLP news engine core
- event-conditioned integration
- calibration workflow
- integration backtest
- integration governance
- daily multi-portfolio watchlist
- live Marketaux validation
- archive-only validation mode
- multi-window live validation harness
- live validation governance
- validation trend reporting
- validation trend governance
- unified live validation suite
- thematic validation symbol packs
- taxonomy hardening for live macro and regulatory news
- entity-linking precision fix for provider false positives
- source-tier policy and quality scoring v2
- operator summary reporting
- retention planning
- Marketaux token redaction in operational logs and failure manifests
- adaptive sync splitting for upstream 402 responses
- archive reuse for validation windows blocked by Marketaux quota
- study docs under `docs/`

## What Is Still Missing

Only the items below remain.

## Remaining Backlog

### 1. Fresh governed history after quota reset

Why:
- archive-only keeps the system operable, but it is not a substitute for new live evidence

Tasks:
- keep running `scripts/run_live_validation_suite.py` on fresh windows once quota resets
- keep archive-only for quota-blocked days only
- grow the fresh governed sample beyond the restored clean streak

Definition of done:
- a larger governed live sample with repeated `PASS`

### 2. Larger calibration and research sample

Why:
- the integration layer is working, but the evidence set is still too small

Tasks:
- gather larger historical event samples
- rerun calibration on broader histories
- store and compare more calibrated snapshots

Definition of done:
- calibration is supported by a wider and more defensible sample

### 3. Fresh live taxonomy and source refinement

Why:
- new providers and live headlines will keep surfacing edge cases

Tasks:
- keep reviewing gap samples from fresh live runs
- add provider-specific overrides where needed
- keep low-signal recap and opinion content out of the watchlist layer

Definition of done:
- fresh live runs keep `other` and suspicious links near zero

### 4. Broader integration backtests

Why:
- the fusion layer should be evaluated on larger event-day sets

Tasks:
- rerun backtests by family and subtype
- compare manual, calibrated, and source-aware maps
- measure where event conditioning helps and where it adds noise

Definition of done:
- the project has a clearer answer for where fusion adds value

### 5. Scenario-map and calibration refinement

Why:
- the mapping layer is richer now, but can still become more realistic

Tasks:
- refine sector overrides with more calibration evidence
- deepen family-specific shock shapes
- keep subtype mappings aligned with real live headlines

Definition of done:
- scenario reactions look differentiated and stable by event family

### 6. Portfolio coverage expansion

Why:
- the current default coverage is broad enough for MVP, but still demo-oriented

Tasks:
- add larger multi-sector books
- optionally group watchlists by theme or operator use case
- keep sector maps and aliases aligned with any new names

Definition of done:
- portfolio ranking is useful beyond validation/demo books

### 7. Reporting and operator UX

Why:
- the operator summary exists, but reporting can still get more decision-friendly

Tasks:
- improve markdown layout
- add concise "why this ranked high" summaries
- add more per-portfolio rollups

Definition of done:
- the latest run is easy to read without opening raw CSVs

### 8. Capital sandbox and pathing simulation

Why:
- the project can already rank risk and explain events, but it still does not simulate a small-balance decision path

Tasks:
- add a paper-trading sandbox with a starting notional such as `R$100`
- simulate choices such as:
  - stay in cash
  - buy one name
  - buy a basket
  - hold
  - de-risk
  - rebalance
- combine:
  - event pressure
  - source quality
  - risk metrics
  - regime state
- include basic friction assumptions:
  - fees
  - slippage
- compare the chosen path against naive alternatives
- log each decision in a readable journal

Definition of done:
- the system can replay and explain a small-capital path day by day
- every decision is tied to a visible rule instead of ad hoc choice

### 9. Automation and operations

Why:
- the scheduler exists, but real recurring operation still needs discipline

Tasks:
- install and use the Windows scheduled task in practice
- define the daily run cadence
- review and optionally apply retention
- add a short failure checklist

Definition of done:
- repeated operation works without manual babysitting

### 10. Observability and run analytics

Why:
- logs exist, but longer-run operational analytics are still thin

Tasks:
- aggregate run health across watchlist and validation histories
- surface retry counts, zero-event windows, and quota pressure trends

Definition of done:
- operational drift is visible without inspecting many run folders

### 11. Live-data persistence and backfill

Why:
- the current archive is usable, but could become a stronger research store

Tasks:
- keep durable archives of live runs
- backfill more historical windows when quota allows
- make replay and rebuild flows easier

Definition of done:
- research can be rerun from stored live data with less manual work

### 12. Risk-engine v2 backlog

Why:
- the integrated stack is functional, but the pure risk engine can still deepen

Tasks:
- factor-model decomposition
- richer scenario families
- volatility regime tagging
- alternative covariance models

Definition of done:
- risk modeling depth moves beyond the MVP set

### 13. Productization backlog

Why:
- the backend is ready enough for a thin UI, but only after live freshness is stable

Tasks:
- local dashboard
- portfolio upload/edit flow
- latest-run landing page
- export bundle

Definition of done:
- non-technical consumption becomes practical

### 14. Documentation extension

Why:
- the study docs exist, but future layers must stay aligned with them

Tasks:
- keep `docs/` synchronized with structural changes
- add diagrams if a UI/product layer starts

Definition of done:
- the repo stays understandable as it grows

## Current Priority Order

1. fresh governed history after quota reset
2. larger calibration and research sample
3. broader integration backtests
4. capital sandbox and pathing simulation
5. fresh live taxonomy and source refinement
6. reporting and operator UX
7. automation and observability
8. productization only after fresh live stability

## Current Readiness

Current operational baseline:

- live validation: `PASS`
- trend governance: `PASS`
- governed clean-pass streak: `5`
- active `other` rate: `0.0`
- active suspicious-link rate: `0.0`
- validation symbol universe: `41` symbols plus thematic packs
- default watchlist coverage: `15` portfolios
