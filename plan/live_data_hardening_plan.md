# Live Data Hardening Plan

## Objective

Make the live `Marketaux` workflow reliable enough that the watchlist report is usable without manual cleanup.

## Why This Exists

The live run already works end to end, but the first real batch exposed issues that do not show up in the deterministic fixtures:

- false-positive ticker links
- broad market articles attaching to single names too easily
- low-signal articles entering the watchlist
- some events still relying on generic fallback behavior

## Current Problems Seen In Live Data

1. `entity linking`
- provider entities can attach a ticker because it appears somewhere in the body, even when the company is not the main subject
- example pattern: unrelated company article linked to `AAPL`

2. `headline anchoring`
- the linker currently trusts provider symbols too much
- it needs to care more about whether the ticker or company is present in the title or description

3. `market-wide vs single-name`
- some broad market articles mention `AAPL` and `MSFT` and get treated as single-name exposure plus market story
- for watchlist ranking, those should often be treated as macro-first

4. `noise filtering`
- not every fetched article is useful for risk
- we need a minimum quality gate before the article reaches the final report

## Execution Plan

### Phase 1 - Fix entity linking precision

Status:
- completed

Goal:
- reduce false-positive tickers in live news

Tasks:
- require stronger evidence before accepting provider-linked symbols
- upweight title and description mentions
- downweight body-only or weak-highlight links
- reject links with weak `match_score`
- add fallback rule:
  - if article is clearly macro and no title-level company mention exists, prefer broad-market handling over single-name linking

Done criteria:
- obviously unrelated articles stop mapping to `AAPL` or `MSFT`

### Phase 2 - Add document quality gates

Status:
- completed

Goal:
- stop low-signal articles from cluttering the watchlist

Tasks:
- create a minimum filter using:
  - `event_confidence`
  - `link_confidence`
  - title quality
  - source allow/deny heuristics
- flag low-quality events instead of dropping them silently
- keep only watchlist-worthy events in the final ranked report

Done criteria:
- live watchlist top rows are mostly economically meaningful events

### Phase 3 - Improve macro handling

Status:
- completed

Goal:
- treat broad market stories as macro-first instead of accidental single-name stories

Tasks:
- if headline has market-wide language and weak issuer anchoring:
  - route event to `macro`
  - collapse affected tickers to benchmark exposure when appropriate
- add broader macro phrases as needed from live batches

Done criteria:
- market sell-off headlines stop behaving like company-specific news

### Phase 4 - Add live evaluation harness

Status:
- completed

Goal:
- evaluate the live pipeline repeatedly without manual inspection

Tasks:
- write a small live QA script that reports:
  - event type distribution
  - ticker concentration
  - low-quality events
  - suspicious links
- save a compact audit report per run

Done criteria:
- every live batch comes with a quick quality audit

## Order Of Work

1. entity linking precision
2. document quality gates
3. macro-first routing
4. live QA report

## Success Metric

The live pipeline is considered hardened when:

1. false ticker links become rare
2. top watchlist events look economically plausible
3. most live events classify into useful event types
4. the report needs little or no manual cleanup
