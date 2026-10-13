# funding-atlas

Open market-data recorder, event log and research toolkit for perpetual-futures
funding — starting with [Hyperliquid](https://hyperliquid.xyz).

**Status: pre-alpha.** The recorder and storage schema are being built first,
on purpose (see below). Nothing here places orders.

---

## Why this exists

Funding rates are the price of leverage, and on perpetual futures they are paid
out hourly between traders. That makes them one of the few genuinely
structural, measurable phenomena in crypto markets — and there is no open,
complete historical record of them.

The reason is unglamorous: **order-book and funding history cannot be
backfilled.** No API will hand you the state of the book last Tuesday at
14:03:27, or what the predicted funding rate was in the minutes before it
settled. If nobody was recording, that information is simply gone.

So the recorder comes first and everything else comes after. This repository
exists to build that record in the open, and to make it usable:

- **Recorder** — continuous capture of trades, mid prices, L2 book snapshots,
  funding and open interest, with gaps recorded as first-class events rather
  than buried in logs.
- **Event log** — an append-only, replayable audit trail of signals, risk
  verdicts, orders, fills and funding payments.
- **Research** — a backtest engine that runs on the same interfaces as live
  execution, with fees, funding and slippage modelled rather than assumed.
- **Data API + dashboard** — funding across assets and venues, in a form you
  can actually look at.

## Design principles

**One code path, three modes.** A strategy is written once against
`MarketFeed` and `Broker` ([`venues/base.py`](src/funding_atlas/venues/base.py)).
Backtest, paper, testnet and live differ only in which implementations are
injected. There is no separate simulator codebase — that separation is the
usual reason a backtest and a live run disagree.

**Time comes from a Clock, never from `time.time()`.**
([`core/clock.py`](src/funding_atlas/core/clock.py)) In simulation the clock
advances only as the data feed advances it, so a strategy cannot read a price
stamped later than the moment it is reasoning about. The look-ahead guard is
structural, not a review checklist.

**Append-only, never mutated.** ([`core/events.py`](src/funding_atlas/core/events.py))
"Why did it do that?" is answerable only if the sequence of inputs and
decisions can be replayed exactly.

**Gaps are data.** A lost stream is a permanent hole. It is written to the
event log with its cause and duration, and research is expected to refuse a
backtest that crosses a gap in an asset it trades.

**The venue is the source of truth.** Reconciliation compares local state
against the exchange, never the reverse.

**Scoped by account from the first commit.** Every event carries an
`account_id`. Retrofitting multi-tenancy into a single-user core means
rewriting it.

## What is recorded

| Stream | Coverage | Notes |
|---|---|---|
| Funding (current + predicted) | all listed perps | sampled every 60s; hourly settlement on HL |
| Trades | all listed perps | full tape |
| Mid prices | all listed perps | 1s samples |
| L2 book snapshots | configurable subset | 20 levels/side, 500ms — the volume driver |
| Open interest | all listed perps | — |

Book depth is opt-in per asset ([`config/recorder.yaml`](config/recorder.yaml))
because it dominates storage. Widening the list later is cheap; filling the
disk in week one and losing the stream is not.

## Quickstart

```bash
git clone https://github.com/iq444/funding-atlas && cd funding-atlas
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env
fa-record --config config/recorder.yaml
```

Public market data requires no credentials. This repository holds no keys and
never will — execution lives in a separate, private engine.

## Storage layout

Hot data is Parquet on local disk, partitioned by stream, asset and date, and
queried with DuckDB. Postgres holds operational state and the event log. Cold
partitions are offloaded to S3-compatible object storage.

```
data/
  trades/venue=hyperliquid/asset=BTC/date=2026-10-13/*.parquet
  l2/venue=hyperliquid/asset=BTC/date=2026-10-13/*.parquet
  funding/venue=hyperliquid/date=2026-10-13/*.parquet
  events/account=<id>/date=2026-10-13/*.jsonl
```

Parquet + DuckDB rather than a time-series database: research queries are
columnar scans over immutable history, which is what this layout is for, and
it costs nothing to run.

## Operations

Credential layout — which host holds what, and why — is in
[docs/deploy-keys.md](docs/deploy-keys.md). Servers get read-only deploy keys
generated on the host itself; only the workstation holds write access, on a
token that expires.

## Roadmap

- [x] Repository skeleton, clock, event schema, venue interfaces
- [ ] Hyperliquid WebSocket + REST feed, gap detection, Parquet writers
- [ ] Data API and public funding dashboard
- [ ] Backtest engine with fee / funding / slippage models
- [ ] Research: carry screener, funding regime analysis
- [ ] Multi-venue feeds (funding dispersion)

## Scope

This is research and data infrastructure. It makes no claim that any strategy
derived from this data is profitable, and it is not investment advice. Neither
better data nor automation removes market, liquidation, counterparty or
operational risk.

## Licence

Apache-2.0. See [LICENSE](LICENSE).
