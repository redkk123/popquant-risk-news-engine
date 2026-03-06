# Aggressive NLP News Engine Plan

Date: 2026-03-05
Project root: `D:\Playground\popquant_1_month`

Primary source:

- `Marketaux`

Official references used:

- docs: [Marketaux Documentation](https://www.marketaux.com/documentation)
- pricing: [Marketaux Pricing](https://www.marketaux.com/pricing)

Verified source facts used in this track:

- `GET /v1/news/all`
- query filters include `symbols`, `language`, `published_before`, `published_after`
- free tier exists and is enough for a low-volume baseline engine

## Current Status

Already implemented:

- Marketaux client
- local raw document repository
- fixture-driven offline sync mode
- canonical document normalization
- duplicate detection by canonical URL and title hash
- fallback ticker linking from alias table
- deterministic event taxonomy
- polarity scoring
- severity scoring
- end-to-end pipeline CLI
- labeled evaluation CLI
- pytest coverage for pipeline and rules

Current runnable entrypoints:

- `scripts/run_news_sync.py`
- `scripts/run_event_pipeline.py`
- `scripts/run_news_engine.py`
- `scripts/run_news_evaluation.py`

Current output families:

- `datasets/raw_news/`
- `datasets/processed_news/`
- `output/news_sync/`
- `output/news_pipeline/`
- `output/news_engine/`
- `output/news_evaluation/`

Latest observed result:

- fixture pipeline produced `6` raw articles
- dedupe reduced them to `5` canonical events
- labeled evaluation is currently `100%` event-type accuracy and `100%` ticker-link accuracy on the demo set

Interpretation:

- the deterministic baseline is strong enough to start integration
- the current engine is useful for portfolio-linked event detection
- the next work is scale, calibration, and tighter linkage into the risk engine

## Mission

Build a reliable event engine that can convert raw financial news into normalized, portfolio-joinable event records.

This track is not about fancy language modeling. It is about:

- ingestion that does not fall apart
- deterministic event classification
- explicit ticker linkage
- auditable severity and polarity scores
- machine-readable outputs another system can trust

## End State

The NLP layer is considered ready when it can answer:

- what relevant news hit my tickers today
- which event type each article belongs to
- how severe the event is
- whether the tone is positive or negative
- whether the link to a ticker is strong enough to drive a scenario

## Build Philosophy

- deterministic before probabilistic
- raw storage before transformation
- explainability before model complexity
- portfolio linkage before summarization
- evaluation before integration

## Core Scope

Required capabilities:

- raw news ingestion
- canonical normalization
- deduplication
- ticker linking
- event taxonomy
- polarity scoring
- severity scoring
- evaluation on a labeled set
- machine-readable event outputs

Deferred on purpose:

- custom LLM classification
- multilingual expansion
- abstractive summarization
- real-time websockets
- frontend news dashboard

## System Layout

```text
popquant_1_month/
  event_engine/
    ingestion/
      marketaux_client.py
      sync_news.py
    parsing/
      dedupe.py
      normalize.py
    nlp/
      entity_linking.py
      taxonomy.py
      sentiment.py
      severity.py
    storage/
      schemas.py
      repository.py
    evaluation.py
    pipeline.py
  config/
    news_entity_aliases.csv
  datasets/
    fixtures/
    labeled_events/
    raw_news/
    processed_news/
  scripts/
    run_news_sync.py
    run_event_pipeline.py
    run_news_engine.py
    run_news_evaluation.py
  output/
    news_sync/
    news_pipeline/
    news_engine/
    news_evaluation/
  tests/
```

## Workstream 1 - Source Access and Raw Storage

Goal:
- make acquisition stable before classification begins

Build:

- Marketaux client with retries
- raw document persistence
- local fixture ingestion for offline development

Files:

- `event_engine/ingestion/marketaux_client.py`
- `event_engine/ingestion/sync_news.py`
- `event_engine/storage/repository.py`

Current execution status:

- implemented

## Workstream 2 - Canonicalization and Dedupe

Goal:
- reduce raw API variability into stable internal documents

Build:

- canonical document schema
- URL normalization
- duplicate detection by URL and title hash
- source and payload lineage retention

Files:

- `event_engine/parsing/normalize.py`
- `event_engine/parsing/dedupe.py`
- `event_engine/storage/schemas.py`

Current execution status:

- implemented

## Workstream 3 - Ticker Linking

Goal:
- connect articles to tradable objects

Build:

- use provider symbols when present
- alias-table fallback from text
- explicit link confidence

Files:

- `event_engine/nlp/entity_linking.py`
- `config/news_entity_aliases.csv`

Current execution status:

- implemented

## Workstream 4 - Event Taxonomy

Goal:
- convert articles into risk-usable event types

Current taxonomy:

- `earnings`
- `guidance`
- `downgrade`
- `upgrade`
- `macro`
- `legal`
- `m_and_a`
- `management`
- `product`
- `other`

Files:

- `event_engine/nlp/taxonomy.py`

Current execution status:

- implemented
- tuned so `earnings` is not incorrectly swallowed by `guidance`

## Workstream 5 - Polarity and Severity

Goal:
- rank articles by expected risk relevance

Build:

- lexical polarity scoring
- optional provider entity sentiment incorporation
- event-type-aware severity scoring
- explanation fields for each score

Files:

- `event_engine/nlp/sentiment.py`
- `event_engine/nlp/severity.py`

Current execution status:

- implemented

## Workstream 6 - End-to-End Pipeline

Goal:
- produce a one-command engine instead of loose helper scripts

Build:

- normalize raw docs
- dedupe
- link tickers
- classify event type
- score polarity and severity
- export canonical docs and events

Files:

- `event_engine/pipeline.py`
- `scripts/run_event_pipeline.py`
- `scripts/run_news_engine.py`

Current execution status:

- implemented

## Workstream 7 - Evaluation

Goal:
- keep the NLP layer measurable instead of subjective

Build:

- labeled event fixture set
- event-type accuracy
- ticker-link accuracy
- detailed mismatch export

Files:

- `event_engine/evaluation.py`
- `datasets/labeled_events/demo_labeled_events.jsonl`
- `scripts/run_news_evaluation.py`

Current execution status:

- implemented
- current demo accuracy: `100%` event type, `100%` ticker link

## Acceptance Criteria

The NLP engine is integration-ready if:

- raw documents can be synced or loaded offline
- canonical docs are reproducible
- duplicates are traceable
- events are normalized and machine-readable
- ticker linking is explicit
- evaluation exists and is repeatable

## Hard Rules

- no opaque classifier as the primary engine
- no event output without a confidence field
- no hidden source transformations
- no integration with risk without stable event contracts

## Immediate Next Build

1. Add source-quality weighting and domain reputation config
2. Expand labeled set beyond the demo fixture
3. Add sector and portfolio intersection tagging
4. Add event-to-scenario mapping hooks for integration
5. Add optional live Marketaux smoke test when API key exists

