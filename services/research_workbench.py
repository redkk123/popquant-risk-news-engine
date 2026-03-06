from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from data.loaders import load_prices
from data.positions import load_portfolio_config, weights_series
from data.validation import validate_price_frame
from event_engine.ingestion.sync_news import ingest_fixture
from event_engine.pipeline import process_raw_documents
from event_engine.storage.repository import NewsRepository
from fusion.calibration import (
    build_calibrated_event_mapping,
    build_event_impact_observations,
    summarize_event_impacts,
    summarize_sector_peer_impacts,
)
from fusion.calibration_registry import (
    build_snapshot_id,
    compare_calibration_snapshots,
    rebuild_calibration_registry,
    write_calibration_snapshot,
)
from fusion.integration_backtest import (
    run_event_conditioned_backtest,
    summarize_event_conditioned_backtest,
    summarize_event_conditioned_backtest_groups,
)
from fusion.mapping_variants import load_mapping_variants
from fusion.scenario_mapper import load_event_mapping_config
from fusion.sector_mapping import load_ticker_sector_map, select_sector_peer_symbols
from services.pathing import PROJECT_ROOT, load_watchlist_paths


SUPPORTED_GROUP_COLUMNS = ("event_type", "event_subtype", "story_bucket", "source_tier")


def _render_integration_backtest_report(
    *,
    summary: dict[str, Any],
    group_outputs: dict[str, pd.DataFrame],
    variant_compare: pd.DataFrame,
    portfolio_compare: pd.DataFrame,
    best_variant_by_group: dict[str, pd.DataFrame],
) -> str:
    lines = [
        "# Integration Backtest Report",
        "",
        f"Portfolios: `{summary.get('portfolio_count', 0)}`",
        f"Event rows: `{summary.get('n_event_rows', 0)}`",
        f"Event days: `{summary.get('n_event_days', 0)}`",
        f"Variants: `{', '.join(summary.get('mapping_variants', [])) or 'configured'}`",
        "",
        "## Overall",
        "",
    ]
    for horizon_key, metrics in summary.get("per_horizon", {}).items():
        lines.extend(
            [
                f"### {horizon_key}",
                f"- baseline MAE: `{metrics['baseline_mae']}`",
                f"- stressed MAE: `{metrics['stressed_mae']}`",
                f"- avg VaR uplift: `{metrics['avg_var_uplift']}`",
                f"- improved rows: `{metrics['improved_days']}`",
                f"- worse rows: `{metrics['worse_days']}`",
                "",
            ]
        )

    if not variant_compare.empty:
        lines.extend(["## Variant Compare", ""])
        for _, row in variant_compare.head(9).iterrows():
            lines.append(
                f"- `{row['mapping_variant']}` horizon `{int(row['horizon_days'])}d`: "
                f"baseline MAE `{row['baseline_mae']}`, stressed MAE `{row['stressed_mae']}`"
            )
        lines.append("")

    if not portfolio_compare.empty:
        lines.extend(["## Portfolio Compare", ""])
        for _, row in portfolio_compare.head(10).iterrows():
            lines.append(
                f"- `{row['portfolio_id']}` / `{row['mapping_variant']}` / `{int(row['horizon_days'])}d`: "
                f"stressed MAE `{row['stressed_mae']}`, uplift `{row['avg_var_uplift']}`"
            )
        lines.append("")

    for column, frame in best_variant_by_group.items():
        if frame.empty:
            continue
        lines.extend([f"## Best Variant by {column}", ""])
        for _, row in frame.head(10).iterrows():
            lines.append(
                f"- `{row[column]}` / `{int(row['horizon_days'])}d`: "
                f"best `{row['best_mapping_variant']}` with stressed MAE `{row['best_stressed_mae']}` "
                f"(baseline `{row['best_baseline_mae']}`)"
            )
        lines.append("")

    for column, frame in group_outputs.items():
        if frame.empty:
            continue
        lines.extend([f"## Grouped by {column}", ""])
        for _, row in frame.head(10).iterrows():
            lines.append(
                f"- `{row[column]}` / `{int(row['horizon_days'])}d`: "
                f"`{row['n_events']}` events, baseline MAE `{row['baseline_mae']}`, stressed MAE `{row['stressed_mae']}`"
            )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _summarize_best_variant_by_group(
    backtest_frame: pd.DataFrame,
    *,
    group_column: str,
    horizons: list[int] | tuple[int, ...],
    min_events: int,
) -> pd.DataFrame:
    grouped = summarize_event_conditioned_backtest_groups(
        backtest_frame,
        group_by=[group_column, "mapping_variant"],
        horizons=horizons,
        min_events=min_events,
    )
    if grouped.empty:
        return pd.DataFrame()

    ordered = grouped.sort_values(
        [group_column, "horizon_days", "stressed_mae", "avg_var_uplift"],
        ascending=[True, True, True, True],
    )
    best = ordered.groupby([group_column, "horizon_days"], as_index=False).first()
    best = best.rename(
        columns={
            "mapping_variant": "best_mapping_variant",
            "baseline_mae": "best_baseline_mae",
            "stressed_mae": "best_stressed_mae",
            "avg_var_uplift": "best_avg_var_uplift",
            "improved_days": "best_improved_days",
            "worse_days": "best_worse_days",
            "unchanged_days": "best_unchanged_days",
            "n_events": "n_events",
            "n_event_days": "n_event_days",
            "n_event_rows": "n_event_rows",
            "portfolio_count": "portfolio_count",
        }
    )
    best["mae_improvement"] = best["best_baseline_mae"] - best["best_stressed_mae"]
    return best.sort_values([group_column, "horizon_days"]).reset_index(drop=True)


def _load_event_rows(*, news_fixture: str | Path | None, alias_table: str | Path) -> list[dict[str, Any]]:
    repository = NewsRepository(PROJECT_ROOT)
    if news_fixture:
        ingest_fixture(repository, news_fixture)
        process_raw_documents(repository, alias_path=alias_table)
    events_frame = repository.load_events_frame()
    if events_frame.empty:
        raise RuntimeError("No processed events available.")
    return events_frame.to_dict(orient="records")


def _resolve_portfolio_paths(
    *,
    portfolio_config: str | Path | None = None,
    portfolio_configs: list[str | Path] | None = None,
    watchlist_config: str | Path | None = None,
) -> list[Path]:
    if watchlist_config:
        return load_watchlist_paths(watchlist_config)
    if portfolio_configs:
        return [Path(path) for path in portfolio_configs]
    if portfolio_config:
        return [Path(portfolio_config)]
    return [PROJECT_ROOT / "config" / "portfolios" / "demo_portfolio.json"]


def run_grouped_integration_backtest_workbench(
    *,
    portfolio_config: str | Path | None = None,
    portfolio_configs: list[str | Path] | None = None,
    watchlist_config: str | Path | None = None,
    event_map_config: str | Path | None = None,
    calibrated_event_map_config: str | Path | None = None,
    alias_table: str | Path | None = None,
    ticker_sector_map_path: str | Path | None = None,
    news_fixture: str | Path | None = None,
    start: str = "2023-01-01",
    end: str | None = None,
    alpha: float = 0.01,
    lam: float = 0.94,
    window: int = 252,
    horizons: list[int] | tuple[int, ...] = (1, 3, 5),
    mapping_variants: list[str] | tuple[str, ...] = ("configured",),
    group_by: list[str] | tuple[str, ...] = SUPPORTED_GROUP_COLUMNS,
    min_events: int = 1,
    cache_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    paths = _resolve_portfolio_paths(
        portfolio_config=portfolio_config,
        portfolio_configs=portfolio_configs,
        watchlist_config=watchlist_config,
    )
    portfolios: list[dict[str, Any]] = []
    for path in paths:
        metadata, positions = load_portfolio_config(path)
        portfolios.append({"path": str(path), "metadata": metadata, "weights": weights_series(positions)})

    events = _load_event_rows(
        news_fixture=news_fixture or (PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json"),
        alias_table=alias_table or (PROJECT_ROOT / "config" / "news_entity_aliases.csv"),
    )
    ticker_sector_map = load_ticker_sector_map(
        ticker_sector_map_path or (PROJECT_ROOT / "config" / "ticker_sector_map.csv")
    )
    requested_symbols = sorted(
        {
            *(ticker for portfolio in portfolios for ticker in portfolio["weights"].index.tolist()),
            *(
                portfolio["metadata"].get("benchmark")
                for portfolio in portfolios
                if portfolio["metadata"].get("benchmark")
            ),
        }
    )
    prices = load_prices(
        tickers=requested_symbols,
        start=start,
        end=end or pd.Timestamp.today().date().isoformat(),
        cache_dir=cache_dir or (PROJECT_ROOT / "data" / "cache"),
    )
    prices = validate_price_frame(prices)

    mapping_payloads = load_mapping_variants(
        base_mapping_path=event_map_config or (PROJECT_ROOT / "config" / "event_scenario_map.yaml"),
        calibrated_mapping_path=calibrated_event_map_config,
        variants=list(mapping_variants),
    )

    backtest_frames: list[pd.DataFrame] = []
    for portfolio in portfolios:
        portfolio_symbols = portfolio["weights"].index.tolist()
        benchmark = portfolio["metadata"].get("benchmark")
        required_columns = portfolio_symbols + ([benchmark] if benchmark else [])
        portfolio_prices = prices.loc[:, required_columns]
        for variant_name, mapping_config in mapping_payloads.items():
            frame = run_event_conditioned_backtest(
                prices=portfolio_prices,
                weights=portfolio["weights"],
                events=events,
                mapping_config=mapping_config,
                ticker_sector_map=ticker_sector_map,
                alpha=alpha,
                lam=lam,
                window=window,
                portfolio_id=portfolio["metadata"]["portfolio_id"],
                benchmark_name=benchmark,
                horizons=horizons,
                mapping_variant=variant_name,
            )
            if not frame.empty:
                frame["portfolio_config_path"] = portfolio["path"]
                backtest_frames.append(frame)

    backtest_frame = pd.concat(backtest_frames, ignore_index=True) if backtest_frames else pd.DataFrame()
    summary = summarize_event_conditioned_backtest(backtest_frame, horizons=horizons)
    summary["requested_group_columns"] = [column for column in group_by if column in SUPPORTED_GROUP_COLUMNS]
    summary["portfolio_ids"] = [portfolio["metadata"]["portfolio_id"] for portfolio in portfolios]

    grouped = {
        column: summarize_event_conditioned_backtest_groups(
            backtest_frame,
            group_by=[column],
            horizons=horizons,
            min_events=min_events,
        )
        for column in summary["requested_group_columns"]
    }
    best_variant_by_group = {
        column: _summarize_best_variant_by_group(
            backtest_frame,
            group_column=column,
            horizons=horizons,
            min_events=min_events,
        )
        for column in summary["requested_group_columns"]
    }
    variant_compare = summarize_event_conditioned_backtest_groups(
        backtest_frame,
        group_by=["mapping_variant"],
        horizons=horizons,
        min_events=min_events,
    )
    portfolio_compare = summarize_event_conditioned_backtest_groups(
        backtest_frame,
        group_by=["portfolio_id", "mapping_variant"],
        horizons=horizons,
        min_events=min_events,
    )

    result = {
        "backtest_frame": backtest_frame,
        "summary": summary,
        "grouped": grouped,
        "best_variant_by_group": best_variant_by_group,
        "variant_compare": variant_compare,
        "portfolio_compare": portfolio_compare,
        "report_markdown": _render_integration_backtest_report(
            summary=summary,
            group_outputs=grouped,
            variant_compare=variant_compare,
            portfolio_compare=portfolio_compare,
            best_variant_by_group=best_variant_by_group,
        ),
        "output_root": None,
        "outputs": {},
    }
    if output_dir is not None:
        run_id = pd.Timestamp.now(tz="UTC").strftime("%Y%m%dT%H%M%SZ")
        output_root = Path(output_dir) / run_id
        output_root.mkdir(parents=True, exist_ok=True)
        output_paths = {
            "timeseries_csv": output_root / "integration_backtest_timeseries.csv",
            "summary_json": output_root / "integration_backtest_summary.json",
            "variant_compare_csv": output_root / "integration_backtest_variant_compare.csv",
            "portfolio_compare_csv": output_root / "integration_backtest_portfolio_compare.csv",
            "report_md": output_root / "integration_backtest_report.md",
        }
        backtest_frame.to_csv(output_paths["timeseries_csv"], index=False)
        variant_compare.to_csv(output_paths["variant_compare_csv"], index=False)
        portfolio_compare.to_csv(output_paths["portfolio_compare_csv"], index=False)
        for column, frame in grouped.items():
            output_paths[f"group_{column}_csv"] = output_root / f"integration_backtest_by_{column}.csv"
            frame.to_csv(output_paths[f"group_{column}_csv"], index=False)
        for column, frame in best_variant_by_group.items():
            output_paths[f"best_variant_by_{column}_csv"] = output_root / f"integration_backtest_best_variant_by_{column}.csv"
            frame.to_csv(output_paths[f"best_variant_by_{column}_csv"], index=False)
        summary["outputs"] = {key: str(path) for key, path in output_paths.items()}
        with output_paths["summary_json"].open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2)
        with output_paths["report_md"].open("w", encoding="utf-8") as handle:
            handle.write(result["report_markdown"])
        result["output_root"] = str(output_root)
        result["outputs"] = {key: str(path) for key, path in output_paths.items()}
    return result


def run_event_calibration_workbench(
    *,
    portfolio_config: str | Path,
    event_map_config: str | Path | None = None,
    alias_table: str | Path | None = None,
    ticker_sector_map_path: str | Path | None = None,
    news_fixture: str | Path | None = None,
    start: str = "2023-01-01",
    end: str | None = None,
    horizons: list[int] | tuple[int, ...] = (1, 3, 5),
    vol_window: int = 10,
    min_observations: int = 2,
    snapshot_label: str = "default",
    parent_snapshot_id: str | None = None,
    registry_root: str | Path | None = None,
    cache_dir: str | Path | None = None,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:
    repository = NewsRepository(PROJECT_ROOT)
    if news_fixture:
        ingest_fixture(repository, news_fixture)
        process_raw_documents(repository, alias_path=alias_table or (PROJECT_ROOT / "config" / "news_entity_aliases.csv"))

    events_frame = repository.load_events_frame()
    if events_frame.empty:
        raise RuntimeError("No processed events available for calibration.")

    metadata, _ = load_portfolio_config(portfolio_config)
    benchmark = metadata.get("benchmark") or "SPY"
    ticker_sector_map = load_ticker_sector_map(
        ticker_sector_map_path or (PROJECT_ROOT / "config" / "ticker_sector_map.csv")
    )
    event_tickers = sorted(
        {
            str(ticker).upper()
            for tickers in events_frame["tickers"].dropna()
            for ticker in tickers
            if str(ticker).strip()
        }
    )
    sector_peer_symbols = select_sector_peer_symbols(event_tickers=event_tickers, ticker_sector_map=ticker_sector_map)
    unique_tickers = sorted(set(event_tickers) | set(sector_peer_symbols) | {benchmark})

    prices = load_prices(
        tickers=unique_tickers,
        start=start,
        end=end or pd.Timestamp.today().date().isoformat(),
        cache_dir=cache_dir or (PROJECT_ROOT / "data" / "cache"),
    )
    prices = validate_price_frame(prices)

    observations = build_event_impact_observations(
        prices=prices,
        events=events_frame.to_dict(orient="records"),
        benchmark_ticker=benchmark,
        ticker_sector_map=ticker_sector_map,
        horizons=horizons,
        vol_window=vol_window,
    )
    summary = summarize_event_impacts(observations, horizons=horizons, vol_window=vol_window)
    sector_summary = summarize_sector_peer_impacts(observations, horizons=horizons, vol_window=vol_window)

    base_mapping = load_event_mapping_config(event_map_config or (PROJECT_ROOT / "config" / "event_scenario_map.yaml"))
    calibrated_mapping = build_calibrated_event_mapping(
        summary=summary,
        base_mapping_config=base_mapping,
        sector_summary=sector_summary,
        min_observations=min_observations,
        return_horizon=min(horizons),
        vol_window=vol_window,
    )

    created_at = pd.Timestamp.now(tz="UTC")
    run_id = created_at.strftime("%Y%m%dT%H%M%SZ")
    output_root = Path(output_dir or (PROJECT_ROOT / "output" / "event_calibration")) / run_id
    output_root.mkdir(parents=True, exist_ok=True)

    observations_path = output_root / "event_impact_observations.csv"
    summary_path = output_root / "event_calibration_summary.csv"
    sector_summary_path = output_root / "event_sector_calibration_summary.csv"
    mapping_path = output_root / "recommended_event_scenario_map.yaml"
    report_path = output_root / "event_calibration_report.json"
    observations.to_csv(observations_path, index=False)
    summary.to_csv(summary_path, index=False)
    sector_summary.to_csv(sector_summary_path, index=False)
    with mapping_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(calibrated_mapping, handle, sort_keys=False)

    calibration_metadata = calibrated_mapping.get("calibration_metadata", {})
    snapshot_metadata = {
        "snapshot_id": build_snapshot_id(run_id=run_id, label=snapshot_label),
        "run_id": run_id,
        "label": snapshot_label,
        "parent_snapshot_id": parent_snapshot_id,
        "created_at": created_at.isoformat(),
        "news_fixture_or_source": str(news_fixture) if news_fixture else "repository_events_frame",
        "portfolio_config": str(Path(portfolio_config)),
        "event_map_base": str(Path(event_map_config or (PROJECT_ROOT / "config" / "event_scenario_map.yaml"))),
        "horizons": [int(horizon) for horizon in horizons],
        "vol_window": int(vol_window),
        "min_observations": int(min_observations),
        "n_events": int(events_frame["event_id"].nunique()),
        "n_observations": int(len(observations)),
        "n_sector_observations": int(
            len(observations.loc[observations["impact_scope"] == "peer_sector"])
        )
        if not observations.empty and "impact_scope" in observations.columns
        else 0,
        "updated_direction_rules": int(calibration_metadata.get("updated_direction_rules", 0) or 0),
        "updated_sector_rules": int(calibration_metadata.get("updated_sector_rules", 0) or 0),
    }
    snapshot_dir = write_calibration_snapshot(
        registry_root=registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry"),
        snapshot_metadata=snapshot_metadata,
        artifact_paths={
            "observations_csv": observations_path,
            "summary_csv": summary_path,
            "sector_summary_csv": sector_summary_path,
            "recommended_mapping_yaml": mapping_path,
        },
    )
    registry_frame = rebuild_calibration_registry(registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry"))
    report_payload = {
        "n_events": snapshot_metadata["n_events"],
        "n_observations": snapshot_metadata["n_observations"],
        "n_sector_observations": snapshot_metadata["n_sector_observations"],
        "tickers_loaded": unique_tickers,
        "horizons": list(horizons),
        "vol_window": int(vol_window),
        "snapshot": {
            "snapshot_id": snapshot_metadata["snapshot_id"],
            "snapshot_dir": str(snapshot_dir),
            "registry_root": str(registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry")),
            "registry_size": int(len(registry_frame)),
        },
        "outputs": {
            "observations_csv": str(observations_path),
            "summary_csv": str(summary_path),
            "sector_summary_csv": str(sector_summary_path),
            "recommended_mapping_yaml": str(mapping_path),
            "snapshot_metadata_json": str(Path(snapshot_dir) / "snapshot_metadata.json"),
            "registry_csv": str(Path(registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry")) / "registry.csv"),
            "registry_json": str(Path(registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry")) / "registry.json"),
        },
    }
    with report_path.open("w", encoding="utf-8") as handle:
        json.dump(report_payload, handle, indent=2)

    return {
        "events_frame": events_frame,
        "observations": observations,
        "summary": summary,
        "sector_summary": sector_summary,
        "calibrated_mapping": calibrated_mapping,
        "snapshot_metadata": snapshot_metadata,
        "snapshot_dir": str(snapshot_dir),
        "registry_frame": registry_frame,
        "output_root": str(output_root),
        "report_payload": report_payload,
        "outputs": report_payload["outputs"],
    }


def list_calibration_snapshots(registry_root: str | Path | None = None) -> pd.DataFrame:
    return rebuild_calibration_registry(registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry"))


def compare_calibration_snapshots_workbench(
    *,
    left_snapshot_id: str,
    right_snapshot_id: str,
    registry_root: str | Path | None = None,
) -> dict[str, Any]:
    return compare_calibration_snapshots(
        registry_root=registry_root or (PROJECT_ROOT / "output" / "event_calibration_registry"),
        left_snapshot_id=left_snapshot_id,
        right_snapshot_id=right_snapshot_id,
    )
