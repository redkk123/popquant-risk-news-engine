# Event Engine Walkthrough

Main entry points:
- `scripts/run_news_sync.py`
- `scripts/run_event_pipeline.py`
- `scripts/run_news_engine.py`

Core files:
- `event_engine/parsing/normalize.py`
- `event_engine/pipeline.py`
- `event_engine/nlp/entity_linking.py`
- `event_engine/nlp/taxonomy.py`
- `event_engine/quality.py`

Mental model:
- raw article -> cleaned document
- cleaned document -> linked ticker(s)
- linked document -> event type, subtype, polarity, severity
- source policy + quality score -> watchlist eligible or filtered

Important outputs:
- `canonical_documents.jsonl`
- `events.jsonl`
