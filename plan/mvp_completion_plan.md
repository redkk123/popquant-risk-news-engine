# MVP Completion Plan

## Current Status

The MVP is functionally complete:

- quant risk core: complete
- NLP news core: complete
- event-conditioned integration: complete
- calibration workflow: complete
- integration backtest: complete
- integration governance: complete
- daily watchlist workflow: complete

## What Is Still Missing

These items are no longer blockers for the MVP. They are the remaining backlog for a stronger version.

### Data Quality

- replace deterministic fixture-heavy validation with larger historical datasets
- validate live Marketaux sync with a real `MARKETAUX_API_TOKEN`
- add source quality filters and article freshness checks

### Calibration Quality

- calibrate by sector instead of only ticker or market-wide buckets
- separate pre-market, intraday, and post-close reaction windows
- add stronger shrinkage and confidence bands around calibrated shocks

### Validation

- add multi-horizon event backtests using `3d` and `5d`
- test whether event-conditioned stress helps explain realized tail days in wider samples

### Productization

- cleaner report templates for review outside development
- optional API or lightweight web UI

### Operations

- config versioning for selected scenario maps
- scheduled refresh and reporting
- logging and failure handling for live sync

## Definition Of Done For This MVP

The MVP is considered concluded because:

1. portfolio risk runs end to end
2. news events run end to end
3. events map into scenarios
4. the integrated layer is calibrated
5. the integrated layer is backtested
6. the system can select between manual and calibrated mappings automatically
7. the system can produce a ranked multi-portfolio daily watchlist report

## Recommended Next Build

If work resumes later, the next build should focus on:

1. larger historical news sample
2. sector-aware calibration
3. scheduled daily reporting on live news data
