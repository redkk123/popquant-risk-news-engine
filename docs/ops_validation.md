# Ops And Validation Walkthrough

Main entry points:
- `scripts/run_live_validation.py`
- `scripts/run_live_validation_suite.py`
- `scripts/run_live_validation_governance.py`
- `scripts/run_validation_trend_report.py`
- `scripts/run_validation_trend_governance.py`
- `scripts/run_operator_summary.py`

What this layer does:
- checks whether live or archived news batches still look sane
- measures taxonomy drift and suspicious linking
- gates promotion based on recent governed history
- exposes a single operator summary for daily review

Important operational modes:
- `live`: tries fresh sync
- `archive-only`: skips API and reuses matching archived windows

What to look for first:
- `avg_active_other_rate`
- `avg_active_suspicious_link_rate`
- clean pass streak
- fresh vs archive-reuse windows
