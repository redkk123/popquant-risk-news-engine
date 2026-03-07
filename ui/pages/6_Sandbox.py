from __future__ import annotations

import json
import sys
import threading
from pathlib import Path
from json import JSONDecodeError

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from event_engine.live_validation import choose_validation_providers
from services.capital_replay_batch import run_capital_replay_batch_workbench
from services.capital_tracking import build_capital_live_curve_frame, build_capital_live_image_payload
from services.capital_workbench import (
    initialize_capital_live_run,
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
from services.sandbox_time import build_replay_timestamp_defaults


st.title("Capital Sandbox")
st.caption("Configure providers, choose a session shape, and run local paper-trading or delayed replays from one page.")

overview = build_overview_payload(PROJECT_ROOT)
stored_tokens = load_provider_tokens(PROJECT_ROOT)
portfolio_options = [str(path) for path in list_portfolio_paths(PROJECT_ROOT)]
if not portfolio_options:
    st.warning("No portfolio JSON files found under config/portfolios.")
    st.stop()
if "sandbox_auto_refresh_enabled" not in st.session_state:
    st.session_state["sandbox_auto_refresh_enabled"] = False


def _format_portfolio_option(path_str: str) -> str:
    path = Path(path_str)
    label = path.stem
    if path.stem == "btc_test_portfolio":
        return f"{label} (BTC 24/7 test)"
    return label


def _find_portfolio_option(filename: str) -> str | None:
    for option in portfolio_options:
        if Path(option).name == filename:
            return option
    return None


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
        "primary_provider": decision["providers"][0] if decision["providers"] else "",
    }


def _render_replay_timer(*, timestamp_defaults: dict[str, object]) -> None:
    current_timestamp = str(timestamp_defaults["current_timestamp"])
    suggested_timestamp = str(timestamp_defaults["suggested_timestamp"])
    mode_label = str(timestamp_defaults["auto_mode"])
    reference_label = "replay as-of" if mode_label == "newsapi_delayed_24h" else "live/reference time"
    components.html(
        f"""
        <div style="padding:0.8rem 1rem;border:1px solid #2b2b37;border-radius:0.6rem;background:#0f1116;color:#fafafa;">
          <div style="font-size:0.95rem;font-weight:600;margin-bottom:0.5rem;">Timestamp Timer</div>
          <div id="sandbox-clock-now" style="margin-bottom:0.25rem;"></div>
          <div id="sandbox-clock-asof" style="margin-bottom:0.25rem;"></div>
          <div style="opacity:0.75;font-size:0.85rem;">mode: {mode_label}</div>
        </div>
        <script>
        const nowBase = new Date("{current_timestamp}");
        const asOfBase = new Date("{suggested_timestamp}");
        const mountedAt = Date.now();
        const nowEl = document.getElementById("sandbox-clock-now");
        const asOfEl = document.getElementById("sandbox-clock-asof");

        function tick() {{
          const elapsedMs = Date.now() - mountedAt;
          const current = new Date(nowBase.getTime() + elapsedMs);
          const asOf = new Date(asOfBase.getTime() + elapsedMs);
          nowEl.textContent = "computer time: " + current.toLocaleString();
          asOfEl.textContent = "{reference_label}: " + asOf.toLocaleString();
        }}

        tick();
        setInterval(tick, 1000);
        </script>
        """,
        height=120,
    )


def _render_live_countdown(*, status_payload: dict[str, object]) -> None:
    step = int(status_payload.get("step", 0) or 0)
    total_steps = int(status_payload.get("total_steps", 0) or 0)
    interval_seconds = int(status_payload.get("decision_interval_seconds", 60) or 60)
    total_seconds = max(0, total_steps * interval_seconds)
    progress_value = 0.0 if total_steps <= 0 else min(max(step / total_steps, 0.0), 1.0)
    expected_end_at = status_payload.get("expected_end_at")
    session_started_at = status_payload.get("session_started_at")

    st.progress(progress_value, text=f"Live progress: step {step}/{total_steps}")
    if expected_end_at and session_started_at:
        countdown_payload = {
            "expected_end_at": str(expected_end_at),
            "session_started_at": str(session_started_at),
            "total_seconds": total_seconds,
        }
    else:
        remaining_seconds = max(0, (total_steps - step) * interval_seconds)
        countdown_payload = {
            "initial_remaining_seconds": remaining_seconds,
            "total_seconds": total_seconds,
        }
    components.html(
        f"""
        <div style="padding:0.8rem 1rem;border:1px solid #2b2b37;border-radius:0.6rem;background:#0f1116;color:#fafafa;">
          <div style="font-size:0.95rem;font-weight:600;margin-bottom:0.5rem;">Session Countdown</div>
          <div id="sandbox-countdown-remaining" style="font-size:1.1rem;margin-bottom:0.25rem;"></div>
          <div id="sandbox-countdown-total" style="opacity:0.75;font-size:0.85rem;"></div>
        </div>
        <script>
        const payload = {json.dumps(countdown_payload)};
        const mountedAt = Date.now();
        const totalSeconds = payload.total_seconds || 0;
        const remainingEl = document.getElementById("sandbox-countdown-remaining");
        const totalEl = document.getElementById("sandbox-countdown-total");

        function formatSeconds(raw) {{
          const value = Math.max(0, Math.floor(raw));
          const minutes = Math.floor(value / 60);
          const seconds = value % 60;
          return minutes.toString().padStart(2, "0") + ":" + seconds.toString().padStart(2, "0");
        }}

        function tick() {{
          let remaining = 0;
          if (payload.expected_end_at) {{
            const expectedEnd = new Date(payload.expected_end_at).getTime();
            remaining = Math.max(0, Math.ceil((expectedEnd - Date.now()) / 1000));
          }} else {{
            const elapsed = Math.floor((Date.now() - mountedAt) / 1000);
            remaining = Math.max(0, (payload.initial_remaining_seconds || 0) - elapsed);
          }}
          remainingEl.textContent = "remaining: " + formatSeconds(remaining);
          totalEl.textContent = "total session: " + formatSeconds(totalSeconds);
        }}

        tick();
        setInterval(tick, 1000);
        </script>
        """,
        height=110,
    )


def _enable_auto_refresh_if_running(*, status_payload: dict[str, object] | None) -> None:
    if not status_payload:
        return
    if not bool(st.session_state.get("sandbox_auto_refresh_enabled", False)):
        return
    if status_payload.get("status") not in {"running", "completing"}:
        return
    components.html(
        """
        <script>
        setTimeout(function() {
          window.parent.location.reload();
        }, 5000);
        </script>
        """,
        height=0,
    )


def _run_live_sandbox_in_background(**kwargs) -> None:
    with temporary_provider_token_env(kwargs.pop("token_inputs")):
        run_capital_sandbox_workbench(**kwargs)


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
        time_column = "tracking_time" if "tracking_time" in selected.columns else "snapshot_time"
        selected[time_column] = selected[time_column].astype(str)
        curve_frame = selected.pivot(index=time_column, columns="path_name", values="capital")
        st.line_chart(curve_frame)
        with st.expander("Snapshot Table", expanded=False):
            st.dataframe(selected, use_container_width=True, hide_index=True)

    journal_frame = result.get("journal_frame")
    if isinstance(journal_frame, pd.DataFrame) and not journal_frame.empty:
        st.subheader("Quant / Risk Engine")
        quant_columns = [
            column
            for column in [
                "session_step",
                "timestamp",
                "regime",
                "signal_score",
                "quant_confirmation",
                "confirmation_score",
                "path_confirmation",
                "path_confirmation_score",
                "eligible_event_count",
                "positive_events",
                "negative_events",
                "momentum_signal",
                "benchmark_momentum",
                "ewma_ratio",
                "risk_on_allowed",
                "price_timestamp_advanced",
                "refresh_status",
            ]
            if column in journal_frame.columns
        ]
        quant_tail = journal_frame.loc[:, quant_columns].tail(20).copy()
        st.dataframe(quant_tail, use_container_width=True, hide_index=True)
        if {"session_step", "signal_score", "confirmation_score"}.issubset(journal_frame.columns):
            signal_chart = journal_frame.loc[
                :,
                ["session_step", "signal_score", "confirmation_score"],
            ].copy()
            signal_chart = signal_chart.drop_duplicates(subset=["session_step"]).set_index("session_step")
            st.line_chart(signal_chart)
        with st.expander("Decision Journal", expanded=False):
            st.dataframe(journal_frame.tail(50), use_container_width=True, hide_index=True)

    if result.get("report_markdown"):
        with st.expander("Markdown Report", expanded=False):
            st.markdown(result["report_markdown"])

    _render_live_snapshot_gallery(
        output_root=result.get("output_root"),
        title=f"{title} Visual Tracking",
    )


def _render_live_snapshot_gallery(*, output_root: str | Path | None, title: str) -> None:
    payload = build_capital_live_image_payload(
        project_root=PROJECT_ROOT,
        output_root=output_root,
        image_limit=6,
    )
    if not payload.get("run_root"):
        return

    st.subheader(title)
    st.caption(f"Run: `{payload['run_root']}`")

    status_path = Path(payload["run_root"]) / "live_session_status.json"
    status_payload: dict[str, object] | None = None
    if status_path.exists():
        try:
            with status_path.open("r", encoding="utf-8") as handle:
                status_payload = json.load(handle)
        except JSONDecodeError:
            status_payload = None
    if status_payload:
        session_meta = status_payload.get("session_meta", {}) or {}
        stale_steps = int(session_meta.get("stale_price_steps", 0) or 0)
        current_timestamp = status_payload.get("current_timestamp")
        providers_used = status_payload.get("providers_used", []) or []
        last_refresh_provider = session_meta.get("last_refresh_provider")
        last_refresh_events = int(session_meta.get("last_refresh_events", 0) or 0)
        last_refresh_inserted = int(session_meta.get("last_refresh_inserted", 0) or 0)
        last_refresh_articles_seen = int(session_meta.get("last_refresh_articles_seen", 0) or 0)
        last_refresh_status = session_meta.get("last_refresh_status") or "unknown"
        provider_label = ", ".join(str(value) for value in providers_used) or "none"
        refresh_label = last_refresh_provider or "none"
        st.caption(
            f"Active providers: bootstrap `{provider_label}` | latest refresh `{refresh_label}`"
        )
        st.caption(
            f"Latest refresh status: `{last_refresh_status}` | "
            f"events `{last_refresh_events}` | inserted `{last_refresh_inserted}` | "
            f"articles_seen `{last_refresh_articles_seen}`"
        )
        _render_live_countdown(status_payload=status_payload)
        if stale_steps > 0:
            st.warning(
                f"Feed intraday parado: ultimo candle observado `{current_timestamp}` | "
                f"stale_price_steps = `{stale_steps}`"
            )
        elif current_timestamp:
            st.caption(f"Last observed candle: `{current_timestamp}`")
        _enable_auto_refresh_if_running(status_payload=status_payload)

    live_curve = payload.get("live_equity_curve_png")
    if live_curve:
        st.image(str(live_curve), caption="Live equity curve", use_container_width=True)

    live_curve_frame, axis_label = build_capital_live_curve_frame(run_root=payload["run_root"])
    if live_curve_frame is not None and not live_curve_frame.empty:
        st.caption(f"Accumulated capital by {axis_label}")
        st.line_chart(live_curve_frame, use_container_width=True)

    minute_images = payload.get("minute_snapshot_images") or []
    if minute_images:
        st.caption("Latest minute snapshots")
        columns = st.columns(min(3, len(minute_images)))
        for index, image_path in enumerate(minute_images):
            with columns[index % len(columns)]:
                st.image(str(image_path), caption=image_path.name, use_container_width=True)


def _load_live_status_payload(output_root: str | Path | None) -> dict[str, object] | None:
    if not output_root:
        return None
    status_path = Path(output_root) / "live_session_status.json"
    if not status_path.exists():
        return None
    try:
        with status_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except JSONDecodeError:
        return None


_render_top_state_cards()

config_col, latest_col = st.columns([1.4, 1.0], gap="large")

with config_col:
    token_inputs = _render_token_manager()

    st.subheader("Run Configuration")
    portfolio_default = _find_portfolio_option("btc_test_portfolio.json")
    preset_portfolio_cols = st.columns(2)
    if preset_portfolio_cols[0].button("Use default portfolio", use_container_width=True):
        st.session_state["sandbox_portfolio_path"] = portfolio_options[0]
    if preset_portfolio_cols[1].button("Use BTC 24/7 test", use_container_width=True, disabled=portfolio_default is None):
        if portfolio_default is not None:
            st.session_state["sandbox_portfolio_path"] = portfolio_default

    if "sandbox_portfolio_path" not in st.session_state or st.session_state["sandbox_portfolio_path"] not in portfolio_options:
        st.session_state["sandbox_portfolio_path"] = portfolio_options[0]

    portfolio_path = st.selectbox(
        "Portfolio",
        options=portfolio_options,
        index=portfolio_options.index(st.session_state["sandbox_portfolio_path"]),
        format_func=_format_portfolio_option,
        key="sandbox_portfolio_path",
    )
    if Path(portfolio_path).name == "btc_test_portfolio.json":
        st.caption("BTC test ativo: feed de preco 24/7. Cobertura de noticias pode ser mais fraca do que em equities.")
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

    strategy_preview = {
        "strategy": "fixture",
        "providers": fixture_provider if fixture_mode else "",
        "primary_provider": fixture_provider if fixture_mode else "",
    }
    if fixture_mode:
        st.info("Fixture mode bypasses live providers and ignores token config.")
    else:
        strategy_preview = _provider_strategy_preview(
            providers=providers,
            mode=mode,
            as_of_timestamp=st.session_state.get("sandbox_as_of_timestamp", ""),
            end=end,
        )
        st.info(
            f"Provider strategy preview: `{strategy_preview['strategy']}` | "
            f"effective order: `{strategy_preview['providers'] or 'none'}`"
        )

    timestamp_defaults = build_replay_timestamp_defaults(
        mode=mode,
        fixture_mode=fixture_mode,
        primary_provider=strategy_preview.get("primary_provider") or None,
    )
    timestamp_context = (
        f"{mode}|{fixture_mode}|{timestamp_defaults['primary_provider']}|{timestamp_defaults['auto_mode']}"
    )
    if st.session_state.get("sandbox_as_of_context") != timestamp_context:
        st.session_state["sandbox_as_of_timestamp"] = timestamp_defaults["suggested_timestamp"]
        st.session_state["sandbox_as_of_context"] = timestamp_context

    as_of_timestamp = st.text_input(
        "Replay as-of timestamp",
        key="sandbox_as_of_timestamp",
        disabled=(mode != "replay_as_of_timestamp"),
        help="NewsAPI uses current computer time minus 24h. Other providers stay on current computer time.",
    )
    if timestamp_defaults["is_newsapi_delayed"]:
        st.caption("NewsAPI ativo: replay as-of alinhado em agora - 24h.")
    elif mode == "replay_as_of_timestamp":
        st.caption("Provider sem delay de 24h: replay as-of alinhado no horário atual do computador.")
    else:
        st.caption("Modo live/replay comum: a referência fica no horário atual do computador.")
    _render_replay_timer(timestamp_defaults=timestamp_defaults)

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
        if mode == "live_session_real_time" and not compare_sessions:
            live_run = initialize_capital_live_run(
                portfolio_config=portfolio_path,
                session_minutes=int(session_minutes),
                decision_interval_seconds=int(decision_interval_seconds),
                output_dir=PROJECT_ROOT / "output" / "capital_sandbox",
                providers=providers,
            )
            st.session_state["sandbox_current_run_root"] = str(live_run["output_root"])
            thread = threading.Thread(
                target=_run_live_sandbox_in_background,
                kwargs={
                    "token_inputs": dict(token_inputs),
                    "portfolio_config": portfolio_path,
                    "mode": mode,
                    "initial_capital": float(initial_capital),
                    "decision_interval_seconds": int(decision_interval_seconds),
                    "session_minutes": int(session_minutes),
                    "news_refresh_minutes": int(news_refresh_minutes),
                    "start": start,
                    "end": end,
                    "news_fixture": fixture_path if fixture_mode else None,
                    "fixture_provider": fixture_provider,
                    "providers": providers,
                    "as_of_timestamp": as_of_timestamp if mode == "replay_as_of_timestamp" else None,
                    "output_dir": PROJECT_ROOT / "output" / "capital_sandbox",
                    "run_id_override": live_run["run_id"],
                    "session_started_at_override": live_run["session_started_at"],
                },
                daemon=True,
            )
            thread.start()
            st.success("Live sandbox started in background. Auto-refresh permanece manual.")
            st.rerun()
        else:
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

st.divider()
refresh_col, _ = st.columns([1.0, 3.0])
with refresh_col:
    st.checkbox(
        "Auto-refresh latest live tracking",
        key="sandbox_auto_refresh_enabled",
        help="Recarrega a pagina a cada 5s enquanto a ultima run live estiver running/completing.",
    )

tracked_run_root = st.session_state.get("sandbox_current_run_root")
tracked_status = _load_live_status_payload(tracked_run_root)
if tracked_status and tracked_status.get("status") in {"starting", "running", "completing"}:
    _render_live_snapshot_gallery(
        output_root=tracked_run_root,
        title="Latest Live Snapshot Tracking",
    )
else:
    st.session_state.pop("sandbox_current_run_root", None)
