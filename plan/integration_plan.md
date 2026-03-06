# Integration Plan

## Objective

Join the quant risk engine and the NLP news engine into one explainable batch workflow that answers:

- which recent events matter for the portfolio
- which holdings are exposed
- what scenario gets activated
- how baseline VaR and ES change after the event-conditioned shock

## Current Status

The first integrated version is already implemented and runnable.

Core files:

- `fusion/scenario_mapper.py`
- `fusion/event_conditioned_risk.py`
- `fusion/reporting.py`
- `config/event_scenario_map.yaml`
- `scripts/run_integrated_risk.py`

Current workflow:

1. Load portfolio config and weights.
2. Refresh processed news from the deterministic fixture or reuse processed events.
3. Load canonical events from the news repository.
4. Map events into scenarios only if they intersect portfolio tickers or are market-wide.
5. Scale shocks by:
   - event type mapping
   - polarity
   - severity
   - recency decay
6. Recompute stressed portfolio risk.
7. Export JSON, CSV, and Markdown outputs.

## General Plan

### Phase 1 - Reproducible batch integration

Status: completed

Delivered:

- one-command integrated run via `scripts/run_integrated_risk.py`
- event-to-portfolio intersection logic
- hand-authored scenario mapping in YAML
- baseline snapshot plus stressed deltas
- machine-readable and human-readable reporting

Primary outputs:

- `output/integration/<run_id>/integrated_report.json`
- `output/integration/<run_id>/integrated_summary.csv`
- `output/integration/<run_id>/integrated_stress_detail.csv`
- `output/integration/<run_id>/integrated_report.md`
- `output/integration/<run_id>/integration_manifest.json`

### Phase 2 - Time-aware impact layer

Status: in progress

Delivered:

- recency decay based on `published_at`
- configurable half-life and max event age in `config/event_scenario_map.yaml`
- report fields for:
  - `event_age_days`
  - `recency_decay`
  - `shock_scale`

Next additions:

- distinguish pre-market, intraday, and post-close windows
- decay by event type instead of one global half-life
- track event freshness explicitly in the manifest

### Phase 3 - Scenario calibration

Status: completed for MVP

Scope:

- calibrate shock sizes with realized post-event returns and volatility jumps
- split sector shocks from single-name shocks
- attach scenario confidence and mapping rationale to every scenario
- create versioned mapping snapshots for reproducibility

Exit criteria:

- scenario library is data-informed, not purely hand-authored
- shock rules are testable and versioned

Implemented entrypoint:

- `scripts/run_event_calibration.py`

### Phase 4 - Integration validation

Status: completed for MVP

Scope:

- compare:
  - baseline risk only
  - news ranking only
  - event-conditioned risk
- measure whether event-conditioned scenarios better explain tail losses
- add simple hit-rate and ranking metrics:
  - event matched to future large move
  - stressed VaR closer to realized tail loss

Exit criteria:

- integrated layer has measurable incremental value

Implemented entrypoint:

- `scripts/run_integration_backtest.py`

### Phase 5 - Integration governance

Status: completed for MVP

Scope:

- compare manual and calibrated scenario maps
- select the active map automatically
- export the selected map as a reproducible artifact

Implemented entrypoint:

- `scripts/run_integration_governance.py`

### Phase 6 - Operator workflow

Status: completed for MVP+

Scope:

- daily watchlist mode
- portfolio summary sorted by risk delta
- top event explanations for each holding
- cleaner report templates for review outside Codex

Implemented entrypoint:

- `scripts/run_daily_watchlist.py`

Scope:

- daily watchlist mode
- portfolio summary sorted by risk delta
- top event explanations for each holding
- cleaner report templates for review outside Codex

Exit criteria:

- one run produces a report that is usable without manual inspection of raw CSVs

## Constraints

Keep the integration:

- batch-based
- deterministic when using fixtures
- explainable
- config-driven

Avoid for now:

- streaming ingestion
- opaque LLM-only mapping
- frontend-heavy work before validation

## Execution Command

```bash
python scripts/run_integrated_risk.py
```

## Success Metric

The integrated layer is ready for productization when:

- event-to-portfolio joins are reliable
- mapping logic is transparent
- stressed deltas are reproducible
- reports explain why risk changed
- validation shows the event-conditioned layer adds signal over baseline risk alone
