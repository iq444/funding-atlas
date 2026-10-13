# Architecture

## The decision that shapes everything else

A trading system has three run modes — backtest, paper, live — and the usual
failure is to implement them as three codebases. The backtest then reports a
number the live system cannot reproduce, and there is no way to tell which one
is lying.

Here, mode is dependency injection:

| Mode | `MarketFeed` | `Broker` | `Clock` |
|---|---|---|---|
| backtest | `ParquetFeed` | `SimBroker` | `SimClock` |
| paper | `HyperliquidFeed` | `SimBroker` | `LiveClock` |
| testnet | `HyperliquidFeed` | `HyperliquidBroker(testnet)` | `LiveClock` |
| live | `HyperliquidFeed` | `HyperliquidBroker(mainnet)` | `LiveClock` |

The strategy, the risk engine and the accounting layer are identical in all
four rows. Everything a backtest exercises is code that will carry real orders.

## Build order, and why it is not the obvious one

The intuitive order is: data → backtest → strategies → execution → risk → live.
That order produces work that has to be thrown away, for two reasons.

**Book data cannot be backfilled.** Recording must begin before anything else
is written, because every day of delay is a day of history that does not exist.
This is the only irreversible cost in the project.

**Venue rules decide which strategies are possible.** Tick sizes, lot sizes,
minimum notionals, margin mechanics, rate limits, reduce-only semantics and
partial-fill behaviour are discovered by sending real orders to a testnet, not
by reading a specification. A backtest written before those are known encodes
assumptions that are simply wrong, and a strategy sized without knowing the
minimum order value can be impossible to hedge accurately.

Hence:

```
0  recorder running 24/7            (data accumulates while the rest is written)
1  core: event log, risk chokepoint, reconciliation, kill switch
2  testnet: real orders, partial fills, disconnects, failure drills
3  backtest on the same interfaces + cost model + statistical validation
4  one strategy end to end
5  dashboard, metrics, monitoring
6  ML regime layer, post-mortem analysis
7  live pilot under hard limits
```

## Data plane vs decision plane

Separated by rate, and therefore by machinery.

**Data plane** — trades, book snapshots, mids. Thousands of records per second,
no per-record validation, straight into Parquet via Arrow. Immutable, columnar,
queried by DuckDB. This is history.

**Decision plane** — signals, risk verdicts, order intents, acks, fills,
funding payments, reconciliation mismatches. A handful of records per minute,
fully validated, append-only. This is the audit trail, and it is rich enough
that both performance accounting and tax reporting are derived from it rather
than reconstructed later.

Keeping them apart means market-data throughput is never paid for in validation
overhead, and audit integrity is never traded away for speed.

## Gaps

A dropped stream is a permanent hole in an asset's history. Treating it as a
log line makes it invisible to research, which then silently backtests across
missing liquidity.

So gaps are events: cause, asset, start, end, duration. The recorder detects
them by silence timeout and by reconnect, writes them before resuming, and
research refuses a run that crosses a gap in an instrument it trades.

## Idempotency

Every order carries a client order id (`cloid`) generated before the first send
attempt and reused on every retry. A timeout does not mean the order was not
accepted — only that the response was lost. Without a stable `cloid`, the retry
is how a flaky connection becomes a doubled position.

## Reconciliation

Local state is a cache; the venue is the truth. A loop compares positions and
open orders against the exchange on an interval. Any mismatch stops new entries
and raises an alert — it does not attempt to be clever about which side is
right.

For a delta-neutral strategy this is not optional. Two legs are only a hedge if
both actually exist at the venue; if one leg filled and the other did not, the
position is directional, and only the exchange can confirm which happened.

## The risk chokepoint

Exactly one function may talk to a `Broker`, and its first action is to run the
risk checks. Not by convention — by structure. There is no second path to the
venue, so a limit cannot be bypassed by a strategy, by a retry handler, or by
an operator in a hurry.

The kill switch is layered: stop new entries, cancel resting orders, close
positions. These are three distinct operations with different risk profiles and
are never collapsed into one button.

A watchdog runs on a different host and cancels everything if the engine stops
sending heartbeats. A dead man's switch on the same machine as the engine dies
with it.

## Deployment

| Stage | Hosts |
|---|---|
| now | 1 small VPS (recorder, Postgres, API) + static dashboard + object storage |
| live | + 1 host for execution, holding keys, no public inbound |
| live | + 1 tiny host for the watchdog |

The box with trading keys is never the box serving a public dashboard.
