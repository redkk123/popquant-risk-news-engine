# GitHub Publish Checklist

Use this when preparing the repo for a first public push.

Current note:

- the project already has its own `.git` folder
- this checklist is now for keeping the public repo clean, not for bootstrapping git

## Keep

- source code under `capital/`, `event_engine/`, `fusion/`, `models/`, `operations/`, `risk/`, `services/`, `simulation/`, `ui/`
- stable configs under `config/`
- small fixture data under `datasets/fixtures/` and `datasets/labeled_events/`
- study docs under `docs/`
- MkDocs site config under `mkdocs.yml` and `.github/workflows/docs.yml`
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
3. Run `mkdocs build --strict`
4. Confirm no secret values appear in:
   - `output/**/run_log.jsonl`
   - `output/**/failure_manifest.json`
   - checked-in fixtures
5. Confirm README commands still match the current scripts
6. Confirm the roadmap file points to the right canonical source:
   - `plan/master_execution_plan.md`
7. Confirm showcase artifacts are lightweight and still representative

## Suggested Reading Order

1. `README.md`
2. `docs/reading_order.md`
3. `docs/architecture.md`
4. `docs/quant_risk.md`
5. `docs/event_engine.md`
6. `docs/fusion.md`
7. `docs/ops_validation.md`
8. `docs/local_ui.md`

## GitHub Pages

If you want the docs site live:

1. keep `mkdocs.yml`, `docs/`, and `.github/workflows/docs.yml` in the repo
2. in repository settings, set:
   - `Pages -> Build and deployment -> Source = GitHub Actions`
