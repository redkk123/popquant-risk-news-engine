from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import pandas as pd


class NewsRepository:
    """File-backed local repository for raw, canonical, and event records."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.raw_dir = self.root / "datasets" / "raw_news"
        self.processed_dir = self.root / "datasets" / "processed_news"
        self.fixtures_dir = self.root / "datasets" / "fixtures"
        self.raw_index_path = self.raw_dir / "raw_index.jsonl"
        self.canonical_path = self.processed_dir / "canonical_documents.jsonl"
        self.events_path = self.processed_dir / "events.jsonl"
        self.ensure_layout()

    def ensure_layout(self) -> None:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        self.fixtures_dir.mkdir(parents=True, exist_ok=True)

    def _raw_payload_file(self, document_id: str) -> Path:
        return self.raw_dir / f"{document_id}.json"

    def existing_raw_document_ids(self) -> set[str]:
        if not self.raw_index_path.exists():
            return set()
        ids: set[str] = set()
        with self.raw_index_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    record = json.loads(line)
                    ids.add(record["document_id"])
        return ids

    def upsert_raw_documents(self, records: Iterable[dict[str, Any]]) -> dict[str, int]:
        existing_ids = self.existing_raw_document_ids()
        inserted = 0
        skipped = 0
        with self.raw_index_path.open("a", encoding="utf-8") as index_handle:
            for record in records:
                document_id = record["document_id"]
                payload_path = self._raw_payload_file(document_id)
                if document_id in existing_ids and payload_path.exists():
                    skipped += 1
                    continue
                payload = record["payload"]
                with payload_path.open("w", encoding="utf-8") as payload_handle:
                    json.dump(payload, payload_handle, indent=2)

                index_record = {
                    "document_id": document_id,
                    "provider": record["provider"],
                    "provider_document_id": record.get("provider_document_id"),
                    "fetched_at": record["fetched_at"],
                    "raw_payload_path": str(payload_path),
                }
                index_handle.write(json.dumps(index_record) + "\n")
                existing_ids.add(document_id)
                inserted += 1
        return {"inserted": inserted, "skipped": skipped}

    def load_raw_documents(self) -> list[dict[str, Any]]:
        if not self.raw_index_path.exists():
            return []

        documents: list[dict[str, Any]] = []
        with self.raw_index_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if not line.strip():
                    continue
                index_record = json.loads(line)
                payload_path = Path(index_record["raw_payload_path"])
                if not payload_path.exists():
                    continue
                with payload_path.open("r", encoding="utf-8") as payload_handle:
                    payload = json.load(payload_handle)
                index_record["payload"] = payload
                documents.append(index_record)
        return documents

    def write_canonical_documents(self, documents: list[dict[str, Any]]) -> Path:
        with self.canonical_path.open("w", encoding="utf-8") as handle:
            for document in documents:
                handle.write(json.dumps(document) + "\n")
        return self.canonical_path

    def write_events(self, events: list[dict[str, Any]]) -> Path:
        with self.events_path.open("w", encoding="utf-8") as handle:
            for event in events:
                handle.write(json.dumps(event) + "\n")
        return self.events_path

    def load_events_frame(self) -> pd.DataFrame:
        if not self.events_path.exists():
            return pd.DataFrame()
        rows = []
        with self.events_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))
        return pd.DataFrame(rows)

