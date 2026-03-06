# Capital Replay Batch Report

Portfolio: `demo_quant_book`
Mode: `replay_as_of_timestamp`
Session minutes: `5`
Decision interval seconds: `60`
Providers: `newsapi`

## Replay Windows

- as_of `2026-03-05T15:30:00-03:00` anchor `2026-03-05T18:30:00+00:00` best `benchmark_hold` final `100.00` strategy `delayed` providers `newsapi`
- as_of `2026-03-05T16:30:00-03:00` anchor `2026-03-05T19:30:00+00:00` best `cash_only` final `100.00` strategy `delayed` providers `none`
- as_of `2026-03-05T17:30:00-03:00` anchor `2026-03-05T20:30:00+00:00` best `cash_only` final `100.00` strategy `delayed` providers `newsapi`
- as_of `2026-03-05T19:04:00-03:00` anchor `2026-03-05T20:59:00+00:00` best `portfolio_hold` final `100.04` strategy `delayed` providers `newsapi`

## Best Windows

- `2026-03-05T19:04:00-03:00` best `portfolio_hold` final `100.04`
- `2026-03-05T15:30:00-03:00` best `benchmark_hold` final `100.00`
- `2026-03-05T16:30:00-03:00` best `cash_only` final `100.00`
- `2026-03-05T17:30:00-03:00` best `cash_only` final `100.00`

## Path Leaderboard

- `cash_only` runs `4` avg `100.00` best `100.00`
- `sector_basket` runs `4` avg `100.00` best `100.00`
- `benchmark_hold` runs `4` avg `99.96` best `100.01`
- `portfolio_hold` runs `4` avg `99.95` best `100.04`
- `capped_risk_long` runs `4` avg `99.92` best `100.00`
- `event_quant_pathing` runs `4` avg `99.92` best `100.00`
- `benchmark_timing` runs `4` avg `99.92` best `99.93`
