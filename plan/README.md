# Planning Index

This folder contains the execution plans for the project expansion.

Execution order:

1. `quant_risk_plan.md`
2. `nlp_news_plan.md`
3. `integration_plan.md`

Rule:

- Finish the core data contracts and validation in one layer before coupling it to the next layer.
- Do not build the web app before the batch pipeline and reports are stable.
- Keep `risk_engine` and `event_engine` independently runnable.

