from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from data.positions import canonicalize_portfolio_payload, write_portfolio_config
from services.pathing import PROJECT_ROOT


def portfolio_config_dir(project_root: str | Path | None = None) -> Path:
    root = Path(project_root) if project_root else PROJECT_ROOT
    return root / "config" / "portfolios"


def list_portfolio_paths(project_root: str | Path | None = None) -> list[Path]:
    root = portfolio_config_dir(project_root)
    return sorted(root.glob("*.json"))


def load_portfolio_payload(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def resolve_portfolio_save_path(
    portfolio_id: str,
    *,
    project_root: str | Path | None = None,
) -> Path:
    portfolio_name = str(portfolio_id).strip()
    if not portfolio_name:
        raise ValueError("portfolio_id must be non-empty.")
    return portfolio_config_dir(project_root) / f"{portfolio_name}.json"


def save_portfolio_payload(
    payload: dict[str, Any],
    *,
    project_root: str | Path | None = None,
    normalize: bool = True,
) -> Path:
    canonical_payload = canonicalize_portfolio_payload(payload, normalize=normalize)
    output_path = resolve_portfolio_save_path(
        canonical_payload["portfolio_id"],
        project_root=project_root,
    )
    return write_portfolio_config(canonical_payload, output_path, normalize=False)
