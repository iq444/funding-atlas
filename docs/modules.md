# Module map — funding-atlas

Every responsibility in the public half of the system, mapped to the file that
owns it. One concern per module; a module that would need a key or place an
order does not belong in this repository at all.

`✅` exists · `◻` planned

## core — the spine

| Module | Owns | Status |
|---|---|---|
| `core/clock.py` | time; `LiveClock` / `SimClock`, the look-ahead guard | ✅ |
| `core/events.py` | event schema, `EventType`, `DataGap` | ✅ |
| `core/eventlog.py` | append-only writer: JSONL on disk + Postgres, per-account sequence | ◻ |
| `core/ids.py` | `cloid` generation, sequence allocation — stable across retries | ◻ |
| `core/config.py` | typed config loading and validation; fails at startup, not mid-run | ◻ |

## venues — the seam

This is where the engine plugs in. Everything below is normalisation; nothing
here signs or sends.

| Module | Owns | Status |
|---|---|---|
| `venues/base.py` | `MarketFeed`, `Broker`, `AssetSpec`, `OrderIntent` protocols | ✅ |
| `venues/normalize.py` | venue payloads → canonical records, one shape for all venues | ◻ |
| `venues/hyperliquid/ws.py` | WebSocket: subscriptions, reconnect with backoff, silence timeout | ◻ |
| `venues/hyperliquid/rest.py` | metadata, asset specs, historical pulls | ◻ |
| `venues/hyperliquid/codec.py` | field mapping and decimal handling — no floats for prices or sizes | ◻ |

Prices and sizes are `Decimal` end to end. Float rounding on a two-leg hedge
produces a position that is nearly neutral, which is the same as directional.

## recorder — the part that cannot be redone later

| Module | Owns | Status |
|---|---|---|
| `recorder/main.py` | supervisor: starts streams, restarts them, owns shutdown | ◻ |
| `recorder/streams.py` | one consumer per stream (trades, mids, L2, funding, OI) | ◻ |
| `recorder/writers.py` | Parquet buffering, rotation, partition paths | ◻ |
| `recorder/integrity.py` | heartbeat, silence detection, gap events, resume | ◻ |
| `recorder/offload.py` | cold partitions → object storage, verify before delete | ◻ |

`integrity.py` is not a utility. A dropped stream is a permanent hole, so it is
detected, written as a `DATA_GAP` event with cause and duration, and surfaced to
research — never swallowed by a retry loop.

## store

| Module | Owns | Status |
|---|---|---|
| `store/parquet.py` | partition layout, path construction, schema versions | ◻ |
| `store/duck.py` | research queries over Parquet; gap-aware range checks | ◻ |
| `store/pg.py` | operational state and event log persistence | ◻ |
| `store/schema/` | Alembic migrations | ◻ |

## research

| Module | Owns | Status |
|---|---|---|
| `research/backtest/engine.py` | replay loop: `ParquetFeed` + `SimBroker` + `SimClock` | ◻ |
| `research/backtest/simbroker.py` | fills, partial fills, queue position, rejects | ◻ |
| `research/backtest/costs.py` | fees, funding accrual, slippage — modelled, never assumed | ◻ |
| `research/backtest/calibrate.py` | refit the cost model on realised fills | ◻ |
| `research/metrics.py` | net P&L, drawdown, expectancy, turnover, time in position | ◻ |
| `research/validation.py` | walk-forward, deflated Sharpe, PBO, multiple-testing correction | ◻ |
| `research/report.py` | strategy reports and comparison | ◻ |

`validation.py` exists because walk-forward alone does not prevent overfitting —
searching parameters over the walk-forward window overfits the window. The
number of attempts is recorded and the result is deflated by it.

`engine.py` refuses a run whose window crosses a `DATA_GAP` in an instrument the
strategy trades. A backtest over invented liquidity is worse than no backtest.

## features

| Module | Owns | Status |
|---|---|---|
| `features/bars.py` | resampling, alignment, session handling | ◻ |
| `features/funding.py` | carry, basis, cross-venue funding dispersion | ◻ |
| `features/regime.py` | regime labels — gradient boosting, versioned, backtestable | ◻ |

No LLM here. Language models explain and review offline; they do not sit in a
path that has to be reproducible.

## api

| Module | Owns | Status |
|---|---|---|
| `api/main.py` | FastAPI app, lifespan, middleware | ◻ |
| `api/routes/funding.py` | funding history, current, dispersion | ◻ |
| `api/routes/assets.py` | universe, specs, data coverage incl. known gaps | ◻ |
| `api/routes/health.py` | recorder liveness, lag, last write per stream | ◻ |
| `api/deps.py` | connections, auth for paid tiers later | ◻ |

The dashboard reads through this API, not through the database. The same path
that serves the public page is the one that would serve a paying customer, so
there is only ever one implementation to keep correct.

## What is deliberately absent

No signing, no key loading, no order submission, no strategy parameters, no risk
limits. Those live in the private engine and reach this repository only through
the protocols in `venues/base.py`.

That boundary is the reason this repository can be public at all.
