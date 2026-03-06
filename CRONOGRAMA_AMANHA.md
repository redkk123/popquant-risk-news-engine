# Cronograma de Continuacao

Data da pausa: 2026-03-05

## Onde paramos

- Projeto movido para `D:\Playground\popquant_1_month`
- Semana 1 implementada:
  - download de precos
  - calculo de log-returns
  - volatilidade EWMA
  - VaR e ES com distribuicao normal
  - grafico e tabelas gerados
- Explicacao conceitual feita em linguagem introdutoria

## Arquivos principais

- `scripts/run_week1.py`
- `data/loaders.py`
- `data/returns.py`
- `models/ewma.py`
- `risk/var.py`
- `risk/es.py`

## Saidas ja geradas

- `output/figures/week1_baseline.png`
- `output/tables/week1_timeseries.csv`
- `output/tables/week1_summary.csv`

## Resultado atual

- Tickers usados: `AAPL`, `MSFT`, `SPY`
- Periodo: `2022-01-01` ate `2026-03-05`
- Observacoes: `1043`
- Violacoes de VaR: `20`
- Taxa observada: `1.92%`
- Taxa alvo: `1.00%`

Interpretacao: o baseline com distribuicao normal subestimou eventos extremos. Isso abre caminho para a Semana 2 com Student-t.

## Proxima sessao

1. Revisar o grafico `week1_baseline.png` com calma e interpretar cada painel.
2. Explicar melhor a relacao entre volatilidade, VaR, ES e violacoes.
3. Comecar a Semana 2:
   - adicionar modelo Student-t
   - recalcular VaR e ES
   - comparar com o baseline normal
4. Revisar os planos em `plan/`:
   - `quant_risk_plan.md`
   - `nlp_news_plan.md`
   - `integration_plan.md`

## Semanas que faltam

### Semana 2 - Caudas grossas com Student-t

Objetivo:
- corrigir a limitacao do modelo normal para eventos extremos

Implementacoes previstas:
- criar modulo `models/student_t.py`
- estimar o parametro `nu` (graus de liberdade)
- recalcular VaR e ES com Student-t
- comparar violacoes entre normal e Student-t

Entregaveis:
- tabela comparativa `normal vs student-t`
- grafico com novas linhas de risco
- interpretacao do impacto nas caudas

### Semana 3 - Shrinkage hierarquico entre ativos

Objetivo:
- estabilizar a estimativa de volatilidade por ativo usando informacao conjunta

Implementacoes previstas:
- criar modulo `models/hierarchical_vol.py`
- estimar volatilidades individuais e volatilidade populacional
- aplicar shrinkage nas estimativas extremas
- comparar `sigma raw` vs `sigma shrinked`

Entregaveis:
- grafico comparando volatilidade bruta e ajustada
- impacto no VaR/ES por ativo e no portfolio
- explicacao intuitiva do modelo hierarquico

### Semana 4 - Portfolio, Monte Carlo e backtest

Objetivo:
- transformar o baseline em um motor de risco mais completo

Implementacoes previstas:
- criar agregacao de risco de portfolio com covariancia
- gerar cenarios de perda com Monte Carlo
- implementar backtest em janela rolante
- adicionar teste de Kupiec para cobertura de VaR

Entregaveis:
- simulacao de perdas do portfolio
- historico de violacoes em rolling window
- numero final do teste de Kupiec
- comparativo consolidado entre os modelos

## Comando para retomar

```powershell
cd D:\Playground\popquant_1_month
python scripts/run_week1.py --tickers AAPL MSFT SPY --start 2022-01-01 --alpha 0.01
```

## Observacao

Se a proxima conversa for mais conceitual, comecar explicando o grafico antes de escrever codigo novo.
