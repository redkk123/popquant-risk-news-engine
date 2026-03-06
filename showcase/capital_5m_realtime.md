# Resultado 5 Minutos Real Time

Run: `20260306T200624303122Z_demo_quant_book`
Modo: `live_session_real_time`
Capital inicial: `100.00`
Duracao real: `5 minutos`
Cadencia de decisao: `60s`

## Resultado Final

- Melhor caminho: `cash_only`
- Capital final do melhor caminho: `100.00`
- `cash_only`: `100.00`
- `sector_basket`: `100.00`
- `event_quant_pathing`: `100.00`
- `capped_risk_long`: `99.87`
- `benchmark_timing`: `99.86`
- `portfolio_hold`: `99.79`
- `benchmark_hold`: `99.72`

## Leitura

- A sessao rodou no relogio real e terminou com `status = completed`.
- Houve `5` refreshes de noticia com `5` sucessos operacionais.
- Nao apareceu evento elegivel para ativar o `event_quant_pathing`.
- Por isso, o melhor path foi simplesmente ficar em caixa.
- Houve `1` passo com preco stale.

## Arquivos Principais

- Relatorio completo: `capital_sandbox_report.md`
- Resumo numerico: `capital_sandbox_summary.csv`
- Snapshots por minuto: `capital_minute_snapshots.csv`
- Journal: `decision_journal.csv`
- Status live: `live_session_status.json`
- Grafico: `capital_sandbox_equity_curve.png`
