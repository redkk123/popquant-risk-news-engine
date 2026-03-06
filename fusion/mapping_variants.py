from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

from fusion.scenario_mapper import load_event_mapping_config
from services.pathing import resolve_latest_selected_map


def neutralize_source_scaling(mapping_config: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of the mapping with source-based scaling disabled."""
    payload = deepcopy(mapping_config)
    settings = payload.setdefault("settings", {})
    source_scaling = settings.setdefault("source_scaling", {})
    tiers = source_scaling.setdefault("tiers", {})
    buckets = source_scaling.setdefault("buckets", {})

    for key in list(tiers.keys()):
        tiers[key] = 1.0
    for key in list(buckets.keys()):
        buckets[key] = 1.0
    return payload


def load_mapping_variants(
    *,
    base_mapping_path: str | Path,
    calibrated_mapping_path: str | Path | None = None,
    variants: list[str] | tuple[str, ...] = ("configured",),
) -> dict[str, dict[str, Any]]:
    """Load named mapping variants for research backtests."""
    requested = [str(variant).strip().lower() for variant in variants if str(variant).strip()]
    if not requested:
        requested = ["configured"]

    base_mapping = load_event_mapping_config(base_mapping_path)
    selected_path = (
        Path(calibrated_mapping_path)
        if calibrated_mapping_path is not None
        else resolve_latest_selected_map(Path(base_mapping_path).resolve().parents[1])
    )
    calibrated_mapping = load_event_mapping_config(selected_path)

    variant_map: dict[str, dict[str, Any]] = {}
    for variant in requested:
        if variant == "configured":
            variant_map[variant] = deepcopy(base_mapping)
        elif variant == "manual":
            variant_map[variant] = neutralize_source_scaling(base_mapping)
        elif variant == "calibrated":
            variant_map[variant] = neutralize_source_scaling(calibrated_mapping)
        elif variant == "source_aware":
            variant_map[variant] = deepcopy(calibrated_mapping)
        else:
            raise ValueError(f"Unsupported mapping variant: {variant}")
    return variant_map
