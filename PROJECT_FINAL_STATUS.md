# Project Final Status

## State

The project is finished from a core implementation standpoint.

What is in place:

- quant risk engine
- risk v2
- deterministic news engine
- multi-provider fallback chain
- event-conditioned risk integration
- calibration registry and snapshot diff
- grouped research backtests
- operator summary and ops analytics
- local Streamlit UI
- real-time capital sandbox
- tests and study docs
- official multi-provider chain:
  - Marketaux
  - The News API
  - NewsAPI.org
  - Alpha Vantage
- NewsAPI.org adapter with ordered fallback support
- live validation now auto-reorders providers by window freshness
- capital sandbox now auto-reorders providers by session freshness
- capital sandbox now supports rigorous `replay_as_of_timestamp` research runs
- replay batch runner for delayed/as-of evidence packs
- Alpha Vantage ticker query plus official macro-topic fallback
- Alpha Vantage burst pacing for the free-tier request limit

## What Is Still Open

The remaining work is mostly external validation rather than missing architecture:

1. more fresh live runs after provider quotas reset
2. more supported fresh samples for promotion coverage metrics
3. promotion of a final guarded map only after more fresh evidence
4. more sandbox validation on sessions with actionable live signal and non-stale price movement

## Latest Evidence

Fresh live evidence improved during the latest cycle:

1. a fresh live-validation window completed using `Alpha Vantage` as the active provider fallback
2. that fresh window produced `49` events and `198` watchlist event rows
3. the guarded candidate map improved `14/15` portfolios on a fresh probe compare against the currently selected map
4. reprocessing that same fresh Alpha-driven batch with the latest taxonomy rules reduced the batch `other` count from `9` to `0`
5. `NewsAPI.org` is now wired and validated end to end: raw sync, watchlist run, and live-validation probe all completed with `newsapi` as the active provider

The promotion gap still remains:

1. grouped aggregate backtests still do not show the guarded lineage beating the pure baseline end-to-end
2. trend governance can still fail after a single provider-quota window, so the live promotion gate is not stable enough yet

## Current External Blockers

- `Marketaux` quota can block fresh validation windows
- `The News API` quota can also block fresh validation windows
- `NewsAPI.org` is delayed on the free plan, so it helps coverage more than same-day freshness
- `Alpha Vantage` is now wired through broader macro topics, but fresh evidence is still constrained by free-tier limits and sparse article coverage for some symbol batches

## Publish Readiness

The repo is publish-ready in structure and documentation, and it is already versioned.

What is already ready:

- local git repository
- `.gitignore`
- `README.md`
- `docs/github_publish.md`
- `docs/reading_order.md`
- `showcase/` with lightweight outputs

## Remaining Promotion Gap

The main remaining gap is not publishing. It is promotion evidence:

1. the guarded integrated map improved archived-live probes but still does not beat the pure baseline in the grouped aggregate
2. fresh governed live history still needs richer supported-event volume
3. the capital sandbox needs more fresh sessions where the event layer actually fires
