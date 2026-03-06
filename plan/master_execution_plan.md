# Master Execution Plan

## Current State

The project already has:

- quant risk engine
- NLP news engine
- event-conditioned risk integration
- model calibration and governance
- live Marketaux watchlist
- live validation suite
- trend-based promotion gate
- source-tier policy and quality scoring v2
- secret redaction in failure manifests and run logs
- adaptive Marketaux sync batching with 402 split fallback
- archive reuse for live-validation windows when upstream quota blocks fresh sync
- archive-only validation mode that skips the API entirely
- thematic validation packs
- operator summary reporting
- retention planning runner
- study docs under `docs/`

Current operational baseline:

- live validation: `PASS`
- trend governance: `PASS`
- governed clean-pass streak: `5`
- recent governed fresh windows are operationally clean but still light on supported events
- active `other` rate: `0.0`
- active suspicious-link rate: `0.0`
- live validation universe: `41` symbols plus thematic packs
- default live watchlist coverage: `15` portfolios

## Full Remaining Roadmap

Everything still open, in execution order:

1. Fresh governed live history
- resume fresh validation runs when upstream quota allows
- keep `live validation` and `trend governance` green on fresh windows, not only archive reuse
- accumulate enough fresh evidence to judge provider and taxonomy changes on new data
- current state: streak rebuilt to `5`, but recent fresh windows still need richer supported event volume

2. Calibration and research expansion
- grow the calibration sample
- run larger event-day backtests
- compare baseline vs manual vs calibrated vs source-aware maps on broader families
- keep versioned calibration snapshots as the measured source of truth

3. Provider and source precision
- continue provider-specific source overrides from fresh live samples
- keep weak recap/opinion/press-release sources out of the active watchlist layer
- tighten live handling of edge-case event families that still show up in real news

4. Capital sandbox expansion
- validate the new richer paths on fresh sessions:
  - `sector_basket`
  - `benchmark_timing`
  - `capped_risk_long`
- promote best-session compare into the operator layer
- make long sessions refresh providers more efficiently under quota pressure
- extend entry gating with path-level performance confirmation

5. Operator layer expansion
- keep operator reporting aligned with live usage
- deepen historical run analytics over longer windows
- surface pathing and compare outputs without opening raw CSVs

6. Productization gate
- only after live freshness and pathing stability are stronger
- continue from the existing local UI rather than opening a new stack
- focus on usability, not architecture churn

7. Repo and GitHub study polish
- keep docs aligned with the real execution layer
- make the sandbox and operator outputs easier to dissect from the repo alone

## Execution Order

### Phase A - Validation History Scale-Up

Status:
- completed

Why:
- the main risk now is not architecture failure, it is lack of longer governed history

Deliverables:
- automated backfill runner for repeated suite execution
- larger governed run history
- rolling summary of validation health

Done:
- `scripts/run_live_validation_backfill.py`
- isolated backfill workspace under `output/backfill_workspace`
- archive reuse for exact-match validation windows when live sync fails due upstream quota

Done:
- trend analytics separate fresh, archive-reuse, and failed windows
- archive-only mode in `scripts/run_live_validation.py`
- archive-only mode in `scripts/run_live_validation_suite.py`
- governed clean-pass streak restored to `5`

Next:
- keep accumulating fresh governed runs once Marketaux quota is available again

### Phase B - Universe Expansion

Status:
- completed

Why:
- more sector coverage means more realistic live-news pressure testing

Done:
- technology
- financials
- energy
- healthcare
- industrials
- digital-assets/financials
- consumer
- internet/platform

Done:
- semis/software split
- defensives
- rates-sensitive basket
- themed validation packs

### Phase C - Taxonomy and Source Policy

Status:
- mostly completed

Why:
- real-news edge cases are the main source of residual noise

Done:
- `config/news_source_policy.yaml`
- source tier, bucket, and adjustment persisted on each event
- stronger penalties for recap/opinion/press-release sources

Done:
- recap, opinion, market-color, and event-driven separation
- new event types:
  - `analyst_note`
  - `regulatory_policy`
  - `product_issue`
  - `supply_chain`
  - `capital_return`
  - `credit_liquidity`
- provider overrides such as `marketbeat` and `stocktitan`
- hard filter for structurally weak recap/opinion/press-release buckets

Next:
- keep refining provider-specific overrides on real live samples

### Phase D - Scenario and Calibration Refinement

Status:
- in progress

Done:
- richer macro sub-types
- subtype-aware scenario mapping
- confidence-aware sector spillover scaling
- source-aware shock scaling
- canonical calibration registry under `output/event_calibration_registry`
- lineage snapshots:
  - `history_v1`
  - `history_v2_conservative`
- backtest-informed family guardrails
- guarded families from grouped research:
  - `guidance`
  - `macro`
- archived-live candidate validation across:
  - `benchmark_heavy_book`
  - `tech_sector_book`
  - `digital_assets_finance_book`
- guarded map reduced average stressed VaR overshoot in all three archived-live comparisons

Next:
- larger calibration sample
- family-level backtest comparisons
- more archived-live and fresh-live comparisons before promotion into the primary selected lineage

### Phase E - Backtest and Research Expansion

Status:
- in progress

Done:
- grouped multi-portfolio backtest across `15` portfolios
- `470` event rows across `130` event days
- compare:
  - `configured`
  - `manual`
  - `calibrated`
  - `source_aware`
- best-variant tables by:
  - `event_type`
  - `event_subtype`
  - `story_bucket`
  - `source_tier`
- guarded re-run showing materially lower overshoot for `macro` and `guidance`
- archived-live probe compare showing guarded-map improvement in `3/3` portfolios

Next:
- larger event-day sample
- more fresh-event evidence to confirm the guarded map outside fixture-backed research
- only promote the guarded map into the main selected lineage after more evidence

### Phase F - Reporting and Operations

Status:
- in progress

Done:
- operator summary script
- rollups by event type, subtype, story bucket, and source tier
- retention planning runner
- validation freshness split exposed in operator summary
- capital compare block exposed in operator summary
- why-ranked labels for top portfolios and events
- ops analytics run tables for:
  - validation
  - watchlist
  - capital sandbox
- capital path leaderboard and refresh-efficiency analytics
- latest ops analytics surfaced in the local UI

Next:
- deeper aggregated run analytics across longer histories
- keep aligning operator reporting with fresh/live evidence once quota allows

### Phase G - Capital Pathing Sandbox

Status:
- in progress

Why:
- the next practical layer is not just measuring risk, but simulating small-capital decision paths
- this adds a bridge between research outputs and actual portfolio choices without jumping straight into real-money trading

Scope:
- simulate a small starting balance such as `R$100`
- decide between:
  - cash
  - single-name exposure
  - sector basket exposure
  - benchmark exposure
  - rebalance / hold / de-risk paths
- use both:
  - news-derived event signals
  - quantitative risk filters

Rules:
- paper-trading first
- include fees and simple slippage assumptions
- no real capital allocation until the sandbox policy is stable

Deliverables:
- capital simulation runner
- decision journal per day
- path comparison report
- benchmark comparison against:
  - stay-in-cash
  - simple benchmark hold
  - naive equal-weight risk-on path

Done when:
- the project can explain, day by day, why it chose one allocation path over another
- path decisions are traceable to both event signals and quant constraints

Done:
- paper-trading runner with:
  - `cash_only`
  - `benchmark_hold`
  - `portfolio_hold`
  - `event_quant_pathing`
- provider-backed event ingestion with fallback chain
- decision journal
- minute snapshots
- replay compare mode for `5m / 15m / 30m`
- local UI page for sandbox runs
- real-time session mode promoted to primary mode
- degraded no-news fallback when the provider chain fails
- live heartbeat files during real-time sessions:
  - `live_session_status.json`
  - `decision_journal.live.csv`
  - `capital_minute_snapshots.live.csv`
- batch launcher for parallel real-time `5m / 15m / 30m`
- PNG exports for sandbox and compare runs
- in-session news refresh for live sessions
- quant confirmation gate before entering risk
- capital sandbox state promoted into:
  - `operator_summary`
  - `ops_analytics`
  - `Overview` in the local UI
- richer path set:
  - `sector_basket`
  - `benchmark_timing`
  - `capped_risk_long`
- path-level performance confirmation in the entry policy
- quota-aware refresh cooldown after provider limit errors
- latest compare surfaced in the operator layer and local UI

Next:
- validate the richer paths on fresh sessions with non-stale price movement
- keep improving long-session refresh efficiency under real quota pressure
- gather more fresh evidence before promoting any path beyond paper-trading

### Phase H - Repo Study Support

Status:
- completed

Done:
- `docs/architecture.md`
- `docs/data_flow.md`
- `docs/reading_order.md`
- `docs/quant_risk.md`
- `docs/event_engine.md`
- `docs/fusion.md`
- `docs/ops_validation.md`
- publication-oriented README updates
- local UI and operator-layer docs aligned with the execution layer

Next:
- add final GitHub publish checklist and artifact-selection guidance

## Concrete Backlog

### 1. Reliability And Live Validation

Scope:
- keep building governed validation history
- keep `live validation` and `trend governance` green
- make quota fallback explicit in analytics

Deliverables:
- more governed `PASS` runs
- trend analytics that separate:
  - fresh sync windows
  - archive-reused windows
  - failed windows
- clearer operator signal on whether a run was live-fresh or quota-reused

Done when:
- repeated governed runs stay green
- fallback behavior is visible and auditable

### 2. Universe And Portfolio Coverage

Scope:
- broaden coverage without destabilizing validation

Done:
- technology
- financials
- energy
- healthcare
- industrials
- digital-assets/financials
- consumer
- internet/platform

Remaining:
- no mandatory sector gaps remain in the current universe
- keep adding larger multi-sector books only when live evidence shows a coverage gap

Done when:
- coverage is broad enough that live validation is no longer concentrated in only a few sectors

### 3. Taxonomy And Source Precision

Scope:
- reduce false positives and low-signal active events

Remaining:
- more recap/commentary rules
- source-specific overrides
- tighter distinctions among:
  - recap
  - opinion
  - market-color
  - true event

Done when:
- active `other` remains near zero
- suspicious-link rates stay near zero
- weak providers rarely survive to the watchlist layer

### 4. Scenario Realism

Scope:
- make event-to-risk mapping more realistic

Remaining:
- macro sub-types
- better single-name vs sector spread logic
- confidence-aware spillover
- richer event families

Done when:
- stressed scenarios look differentiated by event family instead of one-size-fits-all

### 5. Calibration And Research

Scope:
- turn more of the mapping layer into measured rather than manual logic

Remaining:
- larger event sample
- calibration snapshots
- broader event-day research set
- family-level comparison tables

Done when:
- model comparisons are based on larger historical evidence

### 6. Operator Layer

Scope:
- make outputs easier to use without opening raw CSVs

Remaining:
- one-page operator summary
- rollups by `event_type` and `source_tier`
- run analytics dashboard/report
- retention and cleanup rules
- surface capital sandbox compare outputs in the operator layer

Done when:
- someone can understand the latest run without opening multiple raw artifacts

### 7. Capital Pathing Sandbox

Scope:
- simulate a small-capital decision process on top of the existing engine

Tasks:
- define a capital sandbox starting balance such as `R$100`
- simulate daily decisions:
  - stay in cash
  - enter one asset
  - enter a sector/theme basket
  - reduce risk after negative event pressure
  - re-risk after improving conditions
- combine:
  - event severity and polarity
  - source quality
  - portfolio risk state
  - regime state
- log every decision with:
  - selected path
  - rejected alternatives
  - expected risk
  - realized outcome
- compare against naive baselines

Done when:
- the system can produce a readable pathing report for a tiny notional balance
- the path chosen each day is explainable and replayable

### 8. Repo Study Support

Scope:
- make the repo easier to dissect outside Codex

Remaining:
- keep docs aligned as execution changes
- add deeper sandbox/operator walkthroughs when the pathing layer stabilizes

Done when:
- the repo can be studied calmly from GitHub without reverse-engineering the whole flow

## Recommended Next Execution Sequence

1. keep accumulating fresh governed history once quota resets
2. broader integration backtests
3. larger calibration samples
4. capital pathing sandbox in paper mode
5. provider-specific source overrides from fresh live data
6. productization only after live freshness is stable

## Immediate Next Step

The next highest-value move is:

1. continue archive-safe operation while Marketaux is quota-blocked
2. resume fresh governed validation once quota resets
3. expand research backtests with the richer taxonomy and scenario map
4. validate the richer capital sandbox paths on fresh sessions and promote compare results upward

## Active Queue

1. resume fresh governed validation when quota allows
2. expand integration backtests
3. grow calibration sample
4. validate and harden the richer capital sandbox / pathing simulation
5. refine provider-specific source overrides
6. keep operator reporting aligned with live usage

## Rule For Autonomous Execution

When multiple items are open, execute in this order:

1. anything that improves validation reliability
2. anything that broadens live coverage without degrading validation
3. anything that improves scenario realism
4. anything that improves decision simulation and paper-trading quality
5. anything that improves operator usability
6. docs after the execution layer is stable
