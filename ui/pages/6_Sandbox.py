from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.live_validation import choose_validation_providers
from services.capital_replay_batch import run_capital_replay_batch_workbench
from services.capital_workbench import (
    run_capital_sandbox_compare_workbench,
    run_capital_sandbox_workbench,
)
from services.ops_workbench import build_overview_payload
from services.portfolio_manager import list_portfolio_paths
from services.provider_tokens import (
    PROVIDER_ENV_VARS,
    clear_provider_tokens,
    load_provider_tokens,
    provider_token_config_path,
    save_provider_tokens,
    temporary_provider_token_env,
)


st.title("Capital Sandbox")
st.caption("Configure providers, choose a session shape, and run local paper-trading or delayed replays from one page.")

overview = build_overview_payload(PROJECT_ROOT)
stored_tokens = load_provider_tokens(PROJECT_ROOT)
portfolio_options = [str(path) for path in list_portfolio_paths(PROJECT_ROOT)]
if not portfolio_options:
    st.warning("No portfolio JSON files found under config/portfolios.")
    st.stop()


def _provider_token_status() -> pd.DataFrame:
    rows = []
    for provider in ["marketaux", "thenewsapi", "newsapi", "alphavantage"]:
        token = stored_tokens.get(provider, "")
        rows.append(
            {
                "provider": provider,
                "env_var": PROVIDER_ENV_VARS[provider],
                "status": "configured" if token else "missing",
            }
        )
    return pd.DataFrame(rows)


def _render_top_state_cards() -> None:
    latest_capital = (overview.get("latest_operator_summary") or {}).get("capital_sandbox", {})
    latest_compare = (overview.get("latest_operator_summary") or {}).get("capital_compare", {})
    latest_trend = overview.get("latest_trend_governance") or {}
    token_status = _provider_token_status()

    metrics = st.columns(4)
    metrics[0].metric(
        "Latest Live Path",
        latest_capital.get("best_path") or "n/a",
        help="Best path from the latest live sandbox session.",
    )
    metrics[1].metric(
        "Latest Live Capital",
        f"{float(latest_capital.get('best_final_capital', 0.0)):.2f}" if latest_capital else "n/a",
        help="Best final capital from the latest live sandbox session.",
    )
    metrics[2].metric(
        "Latest Compare Winner",
        latest_compare.get("overall_best_path") or "n/a",
        help="Best path from the latest compare run.",
    )
    metrics[3].metric(
        "Configured Tokens",
        int((token_status["status"] == "configured").sum()),
        help="Count of locally configured provider tokens.",
    )

    if latest_trend:
        st.info(
            f"Trend governance: `{latest_trend.get('status', 'unknown')}` | "
            f"Clean pass streak: `{latest_trend.get('clean_pass_streak', 'n/a')}`"
        )


def _render_token_manager() -> dict[str, str]:
    st.subheader("Provider Tokens")
    st.caption(
        f"Saved locally to `{provider_token_config_path(PROJECT_ROOT)}` and ignored by git. "
        "Use this for local testing only."
    )

    token_inputs: dict[str, str] = {}
    cols = st.columns(2)
    ordered = ["marketaux", "thenewsapi", "newsapi", "alphavantage"]
    for index, provider in enumerate(ordered):
        env_var = PROVIDER_ENV_VARS[provider]
        with cols[index % 2]:
            token_inputs[provider] = st.text_input(
                f"{provider} token",
                value=stored_tokens.get(provider, ""),
                type="password",
                help=env_var,
                key=f"token_{provider}",
            )

    action_left, action_right = st.columns(2)
    if action_left.button("Save local token config", use_container_width=True):
        save_provider_tokens(token_inputs, project_root=PROJECT_ROOT)
        st.success("Local provider token config saved.")
        st.rerun()
    if action_right.button("Clear local token config", use_container_width=True):
        clear_provider_tokens(project_root=PROJECT_ROOT)
        st.success("Local provider token config cleared.")
        st.rerun()

    st.dataframe(_provider_token_status(), use_container_width=True, hide_index=True)
    return token_inputs


def _provider_strategy_preview(*, providers: list[str], mode: str, as_of_timestamp: str, end: str) -> dict[str, str]:
    if mode == "replay_as_of_timestamp" and as_of_timestamp.strip():
        published_before = as_of_timestamp.strip()
    elif mode == "historical_daily":
        published_before = end
    else:
        published_before = pd.Timestamp.now(tz="UTC").isoformat()
    decision = choose_validation_providers(providers, published_before=published_before)
    return {
        "strategy": decision["strategy"],
        "providers": ", ".join(decision["providers"]),
    }


def _render_result(result: dict[str, object], *, title: str) -> None:
    st.subheader(title)
    summary_frame = result["summary_frame"]
    if summary_frame.empty:
        st.warning("No result rows were generated.")
        return

    ordered = summary_frame.sort_values("final_capital", ascending=False).reset_index(drop=True)
    best = ordered.iloc[0]
    metrics = st.columns(4)
    metrics[0].metric("Best path", best["path_name"])
    metrics[1].metric("Final capital", f"{float(best['final_capital']):.2f}")
    metrics[2].metric("Total return", f"{float(best['total_return']):.4f}")
    metrics[3].metric("Trades", int(best["trade_count"]))

    st.caption(f"Output: `{result['output_root']}`")
    st.dataframe(ordered, use_container_width=True, hide_index=True)
    st.bar_chart(ordered.set_index("path_name")["final_capital"])

    session_meta = result.get("session_meta") or {}
    if session_meta:
        with st.expander("Session Meta", expanded=False):
            st.json(session_meta)

    snapshot_frame = result.get("snapshot_frame")
    if isinstance(snapshot_frame, pd.DataFrame) and not snapshot_frame.empty:
        st.subheader("Equity Curve")
        available_sessions = (
            snapshot_frame["session_label"].drop_duplicates().tolist()
            if "session_label" in snapshot_frame.columns
            else ["single"]
        )
        selected_session = st.selectbox(
            f"{title} session",
            options=available_sessions,
            index=0,
            key=f"snapshot_session_{title}",
        )
        selected = (
            snapshot_frame.loc[snapshot_frame["session_label"] == selected_session].copy()
            if "session_label" in snapshot_frame.columns
            else snapshot_frame.copy()
        )
        selected["snapshot_time"] = selected["snapshot_time"].astype(str)
        curve_frame = selected.pivot(index="snapshot_time", columns="path_name", values="capital")
        st.line_chart(curve_frame)
        with st.expander("Snapshot Table", expanded=False):
            st.dataframe(selected, use_container_width=True, hide_index=True)

    journal_frame = result.get("journal_frame")
    if isinstance(journal_frame, pd.DataFrame) and not journal_frame.empty:
        with st.expander("Decision Journal", expanded=False):
            st.dataframe(journal_frame.tail(50), use_container_width=True, hide_index=True)

    if result.get("report_markdown"):
        with st.expander("Markdown Report", expanded=False):
            st.markdown(result["report_markdown"])


_render_top_state_cards()

config_col, latest_col = st.columns([1.4, 1.0], gap="large")

with config_col:
    token_inputs = _render_token_manager()

    st.subheader("Run Configuration")
    portfolio_path = st.selectbox("Portfolio", options=portfolio_options, index=0)
    mode = st.selectbox(
        "Mode",
        options=["live_session_real_time", "replay_intraday", "replay_as_of_timestamp", "historical_daily"],
        index=0,
        help="Use live for clock-time runs, replay for quick intraday, replay_as_of_timestamp for rigorous delayed tests.",
    )

    preset_cols = st.columns(3)
    if preset_cols[0].button("Preset 5m", use_container_width=True):
        st.session_state["sandbox_session_minutes"] = 5
    if preset_cols[1].button("Preset 15m", use_container_width=True):
        st.session_state["sandbox_session_minutes"] = 15
    if preset_cols[2].button("Preset 30m", use_container_width=True):
        st.session_state["sandbox_session_minutes"] = 30

    left, middle, right = st.columns(3)
    initial_capital = left.number_input("Initial capital", min_value=10.0, value=100.0, step=10.0)
    interval_options = [60, 120, 300] if mode == "live_session_real_time" else [10, 20, 30, 60]
    decision_interval_seconds = middle.selectbox("Decision interval", options=interval_options, index=0)
    session_minutes = right.selectbox(
        "Session preset",
        options=[5, 15, 30],
        index=[5, 15, 30].index(st.session_state.get("sandbox_session_minutes", 5)),
        key="sandbox_session_minutes",
    )

    as_of_timestamp = st.text_input(
        "Replay as-of timestamp",
        value="2026-03-05T19:04:00-03:00",
        disabled=(mode != "replay_as_of_timestamp"),
        help="Used only in replay_as_of_timestamp mode.",
    )
    news_refresh_minutes = st.number_input(
        "News refresh cadence (minutes)",
        min_value=1,
        value=2,
        step=1,
        disabled=(mode != "live_session_real_time"),
    )
    compare_sessions = st.multiselect(
        "Compare sessions",
        options=[5, 15, 30],
        default=[],
        help="Runs multiple session lengths in one report. Disabled for live mode.",
        disabled=(mode == "live_session_real_time"),
    )

    fixture_mode = st.checkbox(
        "Use fixture instead of live providers",
        value=(mode == "historical_daily"),
        disabled=(mode == "live_session_real_time"),
    )
    fixture_path = st.text_input(
        "Fixture path",
        value=str(PROJECT_ROOT / "datasets" / "fixtures" / "sample_marketaux_news_history.json"),
        disabled=not fixture_mode,
    )
    fixture_provider = st.selectbox(
        "Fixture provider",
        options=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        index=0,
        disabled=not fixture_mode,
    )

    providers = st.multiselect(
        "Provider order",
        options=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        default=["marketaux", "thenewsapi", "newsapi", "alphavantage"],
        disabled=fixture_mode,
        help="The system still reorders these by freshness where applicable.",
    )

    start_end_cols = st.columns(2)
    start = start_end_cols[0].text_input("Historical start", value="2024-01-01")
    end = start_end_cols[1].text_input("Historical end", value="2026-03-06")

    if fixture_mode:
        st.info("Fixture mode bypasses live providers and ignores token config.")
    else:
        preview = _provider_strategy_preview(
            providers=providers,
            mode=mode,
            as_of_timestamp=as_of_timestamp,
            end=end,
        )
        st.info(
            f"Provider strategy preview: `{preview['strategy']}` | "
            f"effective order: `{preview['providers'] or 'none'}`"
        )

with latest_col:
    st.subheader("Latest Runs")
    latest_capital = (overview.get("latest_operator_summary") or {}).get("capital_sandbox", {})
    latest_compare = (overview.get("latest_operator_summary") or {}).get("capital_compare", {})
    if latest_capital:
        st.caption("Latest live session")
        st.json(latest_capital)
    if latest_compare:
        st.caption("Latest compare run")
        st.json(
            {
                "run": latest_compare.get("run"),
                "overall_best_session": latest_compare.get("overall_best_session"),
                "overall_best_path": latest_compare.get("overall_best_path"),
                "overall_best_final_capital": latest_compare.get("overall_best_final_capital"),
            }
        )

run_tab, batch_tab = st.tabs(["Single Run", "Replay Batch"])

with run_tab:
    if st.button("Run Capital Sandbox", disabled=not bool(portfolio_options), use_container_width=True):
        with temporary_provider_token_env(token_inputs):
            if compare_sessions:
                result = run_capital_sandbox_compare_workbench(
                    portfolio_config=portfolio_path,
                    mode=mode,
                    initial_capital=float(initial_capital),
                    decision_interval_seconds=int(decision_interval_seconds),
                    session_minutes_list=compare_sessions,
                    start=start,
                    end=end,
                    news_fixture=fixture_path if fixture_mode else None,
                    fixture_provider=fixture_provider,
                    providers=providers,
                    as_of_timestamp=as_of_timestamp if mode == "replay_as_of_timestamp" else None,
                    output_dir=PROJECT_ROOT / "output" / "capital_sandbox",
                )
            else:
                result = run_capital_sandbox_workbench(
                    portfolio_config=portfolio_path,
                    mode=mode,
                    initial_capital=float(initial_capital),
                    decision_interval_seconds=int(decision_interval_seconds),
                    session_minutes=int(session_minutes),
                    news_refresh_minutes=int(news_refresh_minutes),
                    start=start,
                    end=end,
                    news_fixture=fixture_path if fixture_mode else None,
                    fixture_provider=fixture_provider,
                    providers=providers,
                    as_of_timestamp=as_of_timestamp if mode == "replay_as_of_timestamp" else None,
                    output_dir=PROJECT_ROOT / "output" / "capital_sandbox",
                )
        _render_result(result, title="Capital Sandbox Result")

with batch_tab:
    st.caption("Best used for delayed/time-shifted testing with multiple `as_of` timestamps.")
    batch_timestamps = st.text_area(
        "As-of timestamps (one per line)",
        value="\n".join(
            [
                "2026-03-05T15:30:00-03:00",
                "2026-03-05T16:30:00-03:00",
                "2026-03-05T17:30:00-03:00",
                "2026-03-05T19:04:00-03:00",
            ]
        ),
        height=160,
        disabled=fixture_mode,
    )
    batch_disabled = fixture_mode or not bool(portfolio_options)
    if st.button("Run Replay Batch", disabled=batch_disabled, use_container_width=True):
        as_of_rows = [row.strip() for row in batch_timestamps.splitlines() if row.strip()]
        with temporary_provider_token_env(token_inputs):
            batch_result = run_capital_replay_batch_workbench(
                portfolio_config=portfolio_path,
                as_of_timestamps=as_of_rows,
                initial_capital=float(initial_capital),
                decision_interval_seconds=int(decision_interval_seconds),
                session_minutes=int(session_minutes),
                providers=providers,
                output_dir=PROJECT_ROOT / "output" / "capital_replay_batch",
            )
        st.subheader("Replay Batch Result")
        summary_frame = batch_result["summary_frame"]
        if summary_frame.empty:
            st.warning("Replay batch completed with no rows.")
        else:
            best = summary_frame.sort_values("best_final_capital", ascending=False).iloc[0]
            batch_metrics = st.columns(4)
            batch_metrics[0].metric("Best replay", best["as_of_timestamp"])
            batch_metrics[1].metric("Best path", best["best_path"])
            batch_metrics[2].metric("Final capital", f"{float(best['best_final_capital']):.2f}")
            batch_metrics[3].metric("Replays", len(summary_frame))
            st.caption(f"Output: `{batch_result['output_root']}`")
            st.dataframe(summary_frame, use_container_width=True, hide_index=True)
            st.bar_chart(summary_frame.set_index("as_of_timestamp")["best_final_capital"])
        with st.expander("Replay Batch Report", expanded=False):
            st.markdown(batch_result["report_markdown"])
