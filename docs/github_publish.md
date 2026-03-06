# GitHub Publish Checklist

Use this when preparing the repo for a first public push.

Current note:

- the project directory does not yet have its own `.git` folder
- if you want to publish from `D:\\Playground\\popquant_1_month`, start with `git init`

## Keep

- source code under `capital/`, `event_engine/`, `fusion/`, `models/`, `operations/`, `risk/`, `services/`, `simulation/`, `ui/`
- stable configs under `config/`
- small fixture data under `datasets/fixtures/` and `datasets/labeled_events/`
- study docs under `docs/`
- one lightweight showcase graphic such as `output/figures/week1_baseline.png`

## Exclude

- local virtualenvs and caches
- `data/cache/`
- heavy `output/` run folders
- raw/processed live news archives unless you explicitly want to publish them
- any file that could contain tokens or provider secrets

## Suggested Showcase Outputs

- `output/figures/week1_baseline.png`
- one `operator_summary.md`
- one `ops_analytics_report.md`
- one `capital_compare_report.md`

If you include generated outputs, prefer short markdown reports and small PNGs over full CSV bundles.

## Pre-Push Checks

1. Run `python -m pytest tests -q`
2. Open the Streamlit UI once with `streamlit run ui/app.py`
3. If the repo has not been initialized yet, run `git init`
3. Confirm no secret values appear in:
   - `output/**/run_log.jsonl`
   - `output/**/failure_manifest.json`
   - checked-in fixtures
4. Confirm README commands still match the current scripts
5. Confirm the roadmap file points to the right canonical source:
   - `plan/master_execution_plan.md`

## Suggested Reading Order

1. `README.md`
2. `docs/reading_order.md`
3. `docs/architecture.md`
4. `docs/quant_risk.md`
5. `docs/event_engine.md`
6. `docs/fusion.md`
7. `docs/ops_validation.md`
8. `docs/local_ui.md`
