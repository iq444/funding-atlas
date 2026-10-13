# Hyperliquid API — recorder reference

What the recorder in `venues/hyperliquid/` is coded against. Every field name here
was checked against the official docs or against a live response captured on
2026-10-13 (mainnet); the source is cited per section. Anything not verified is
marked `TODO(verify)` rather than guessed — a wrong field name produces a
recorder that writes nulls for a week and looks healthy while doing it.

Two invariants to internalise before reading the rest:

* **Every numeric market-data value is a JSON string.** `px`, `sz`, `funding`,
  `openInterest`, `oraclePx` — all strings. This is convenient: feed them
  straight to `Decimal`, never through `float`.
* **Every venue timestamp is milliseconds since epoch.** `records.py` stores
  `ts_event_ns`, so the codec multiplies by 1_000_000. There is no nanosecond
  field anywhere in this API; the extra three digits are always zeros and must
  not be mistaken for precision we have.

## 1. WebSocket endpoints and lifecycle

Source: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket>,
<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats>

| Network | URL |
|---|---|
| Mainnet | `wss://api.hyperliquid.xyz/ws` |
| Testnet | `wss://api.hyperliquid-testnet.xyz/ws` |

Lifecycle: connect, send one `subscribe` per stream, read. No auth for market
data. Subscriptions are per-connection and are **not** restored by the server
after a reconnect — resubscribing is the client's job.

**Heartbeat.** The server closes any connection it has not *received* a message
on in the last 60 seconds. The documented keepalive is an application-level
ping, not a WS control frame:

```json
{ "method": "ping" }
```
```json
{ "channel": "pong" }
```

The official Python SDK pings every 50 s
(<https://github.com/hyperliquid-dex/hyperliquid-python-sdk/blob/master/hyperliquid/websocket_manager.py>,
`while not self.stop_event.wait(50)`). Use 50 s; a 60 s period races the server's
own 60 s cutoff.

Note the asymmetry that matters for `integrity.py`: the 60 s timeout is about
messages *we* send. A connection on which we ping faithfully but the venue has
stopped publishing stays open indefinitely and looks alive at the socket layer.
That is exactly the silent-disconnect case `max_silence_seconds: 90` exists to
catch, and it has to be measured on *inbound* data, not on socket state.

**REST over WS.** The same socket accepts info requests, which avoids a second
transport for occasional metadata pulls
(<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests>):

```json
{ "method": "post", "id": 1, "request": { "type": "info", "payload": { "type": "meta" } } }
```
```json
{ "channel": "post", "data": { "id": 1, "response": { "type": "info", "payload": { } } } }
```

`id` must be unique per outstanding request. On error, `response.type` is
`"error"` and `payload` is a string mirroring the HTTP status. `explorer`
requests are not supported over WS.

## 2. Subscription messages

Source: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions>

Uniform envelope: `{"method": "subscribe", "subscription": {...}}`, and
`"unsubscribe"` with the **identical** subscription object to stop.

```json
{ "method": "subscribe", "subscription": { "type": "allMids" } }
{ "method": "subscribe", "subscription": { "type": "trades", "coin": "BTC" } }
{ "method": "subscribe", "subscription": { "type": "l2Book", "coin": "BTC" } }
{ "method": "subscribe", "subscription": { "type": "activeAssetCtx", "coin": "BTC" } }
{ "method": "subscribe", "subscription": { "type": "bbo", "coin": "BTC" } }
{ "method": "subscribe", "subscription": { "type": "candle", "coin": "BTC", "interval": "1m" } }

{ "method": "unsubscribe", "subscription": { "type": "trades", "coin": "BTC" } }
```

Ack (observed live, verbatim):

```json
{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"allMids"}}}
```

Optional parameters:

| Subscription | Optional field | Meaning |
|---|---|---|
| `allMids` | `dex` (string) | HIP-3 dex; omitted ⇒ first perp dex. Spot mids are only included for the first perp dex. |
| `l2Book` | `nSigFigs` (2–5 or null) | price aggregation; null ⇒ full precision |
| `l2Book` | `mantissa` (1, 2, 5) | only valid when `nSigFigs` is 5 |
| `l2Book` | `fast` (bool) | true ⇒ 5 levels; otherwise 20 |

`config/recorder.yaml` asks for `depth: 20`, so leave all three unset: the
default is full precision, 20 levels per side. Setting `nSigFigs` would silently
coarsen recorded prices, which is unrecoverable.

Note that `activeAssetCtx` is **per-coin**. For `mode: all_perps` that is one
subscription per listed perp — see the 1000-subscription cap in §5 — which is
why `config/recorder.yaml` polls funding over REST at `poll_seconds: 60`
instead. `metaAndAssetCtxs` returns the same context for every asset in one
request.

**`TODO(verify)`** — the exact ack for `unsubscribe`. The docs show no example,
and in a live 5 s window after sending one no `subscriptionResponse` with
`"method":"unsubscribe"` arrived (only unrelated stream data). Treat
unsubscription as fire-and-forget and do not block on an ack.

## 3. Inbound message shapes

All captured live on mainnet unless noted. Envelope is always
`{"channel": <name>, "data": ...}`.

### allMids

```json
{"channel": "allMids", "data": {"mids": {"BTC": "83239.5", "@1": "0.1874", "#102220": "0.815"}}}
```

`data.mids` is a flat map, coin name → price string. Live response carried 1294
keys in three naming classes, which the codec must filter:

| Key form | Meaning |
|---|---|
| `BTC`, `ETH`, `HYPE` | perps, by name — the ones we want |
| `@1`, `@1003` | spot pairs, by index |
| `#102220` | numbered markets (HIP-3 / non-perp listings) |

`mids` has **no timestamp**. Use `ts_ingest_ns` and set `ts_event_ns` from it
only if the codec records that it did; a fabricated event time that is
indistinguishable from a real one is worse than an honest equality.
Per the info-endpoint docs, if a book is empty the mid falls back to the last
trade price — so a mid is not always a true `(bid+ask)/2`.

### trades

```json
{"channel": "trades", "data": [
  {"coin": "BTC", "side": "B", "px": "83245.0", "sz": "0.14397",
   "time": 1791893062595,
   "hash": "0xe1375f63789827e8e2b104467803bc02019d0049139b46ba85000ab6379c01d3",
   "tid": 926762701686957,
   "users": ["0x75c3f4a9f37194d42aa141fe5e7b640c48f9dca3", "0xa62b923a112d50d03e1e096bbd53422490dac104"]}
]}
```

`data` is an **array** — one message can carry many trades (a block's worth).

| Field | Type | Notes |
|---|---|---|
| `coin` | string | |
| `side` | `"A"` \| `"B"` | **aggressor side.** `B` = bid = buy, `A` = ask = sell/short. Map to `records.Side`: `B → "buy"`, `A → "sell"`. |
| `px`, `sz` | string | `sz` in base units of the coin |
| `time` | int | ms |
| `hash` | string | L1 transaction hash |
| `tid` | int (JSON number) | 50-bit hash of the buyer and seller order ids |
| `users` | `[string, string]` | `[buyer, seller]` addresses |

Side encoding and its aggressor meaning are documented
(<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/notation>:
"B — Bid, i.e. Buy", "A — Ask, i.e. Short", "Side is aggressing side for
trades") and confirmed live — observed `side: "B"` prints at the ask (83245)
and `side: "A"` at the bid (83244), which is the aggressor, not the resting
side.

`trade_id` in `records.Trade` is a string. Use `tid`, but the docs warn it is
only a 50-bit hash: **not globally unique on its own.** For a unique key
combine it with coin and block time, i.e. `f"{coin}:{time}:{tid}"`. `hash` is
per-transaction and is shared by every fill in that transaction, so it is not a
trade id either.

### l2Book

```json
{"channel": "l2Book", "data": {
  "coin": "BTC",
  "time": 1791893079509,
  "levels": [
    [{"px": "83265.0", "sz": "3.95272", "n": 17}, {"px": "83264.0", "sz": "0.00014", "n": 1}],
    [{"px": "83266.0", "sz": "5.75263", "n": 15}]
  ]}}
```

`levels` is a **two-element array, not an object**: `levels[0]` is bids,
`levels[1]` is asks. The docs do not state this; it was confirmed against a live
mainnet response — `levels[0]` descends from 83239 and `levels[1]` ascends from
83240, i.e. side 0 is the lower-priced side. Both arrays are best-first
(bids descending, asks ascending), 20 entries each at default settings.

`WsLevel`: `px` (string), `sz` (string, total size at that price), `n` (int,
number of orders). `n` maps to `BookLevel.n_orders`.

`time` is ms, stamped by the venue. Updates are pushed per block once enough
time has elapsed since the last push, so cadence is irregular and
`sample_ms: 500` is a downsample of a stream that is not itself fixed-rate.

**Each message is a full top-20 snapshot of both sides**, not a delta. That is
precisely the property `BookSnapshot` is built on: a dropped message costs one
sample, never corrupts the following state. Do not implement increment
application; there is nothing to apply.

### activeAssetCtx

```json
{"channel": "activeAssetCtx", "data": {
  "coin": "BTC",
  "ctx": {
    "funding": "0.0000035092",
    "openInterest": "38521.32876",
    "prevDayPx": "83367.0",
    "dayNtlVlm": "3307803395.8045959473",
    "premium": "-0.0005041533",
    "oraclePx": "83308.0",
    "markPx": "83268.2",
    "midPx": "83265.5",
    "impactPxs": ["83265.0", "83266.0"],
    "dayBaseVlm": "39967.9110500001"
  }}}
```

| Field | Type | Units / meaning |
|---|---|---|
| `funding` | string | current hourly funding rate, as a fraction (see §6) |
| `openInterest` | string | **base units of the coin**, not USD notional |
| `oraclePx` | string | validator oracle price |
| `markPx` | string | mark price |
| `midPx` | string \| null | can be null (HIP-3 markets) |
| `premium` | string \| null | premium index, fraction of oracle |
| `impactPxs` | `[string, string]` \| null | `[impact bid, impact ask]` |
| `prevDayPx` | string | |
| `dayNtlVlm` | string | 24h volume, USD notional |
| `dayBaseVlm` | string | 24h volume, base units |

There is **no timestamp in `ctx`**. `FundingSample.ts_event_ns` and
`OpenInterest.ts_event_ns` therefore come from our clock for this path, same
caveat as `allMids`. Spot assets arrive on channel `activeSpotAssetCtx` with
`circulatingSupply` instead of `funding`/`openInterest`/`oraclePx`.

### bbo

```json
{"channel": "bbo", "data": {"coin": "BTC", "time": 1791893079777,
  "bbo": [{"px": "83265.0", "sz": "6.41054", "n": 20}, {"px": "83266.0", "sz": "5.75263", "n": 15}]}}
```

`bbo` is `[best bid, best ask]`, either element nullable. Emitted only when the
BBO changes on a block, so it is far cheaper than `l2Book` — worth considering
for assets not on the `l2_book.assets` list, since a BBO series answers most
spread questions at a fraction of the disk.

### candle

`data` is an array of candle objects, fields as in §4's `candleSnapshot`.

### Snapshot flag

The subscriptions doc states that time-series feeds begin with a message tagged
`isSnapshot: true`, which may be skipped if earlier data is already processed.

**`TODO(verify)`** — no `isSnapshot` field appeared on any live `l2Book`,
`trades`, `bbo`, `activeAssetCtx` or `allMids` message in a 20 s capture. It is
unclear whether the flag applies only to `candle`/user feeds, or whether the
first `trades` array after subscribing is a backfill of recent trades (it did
arrive as a multi-trade batch with timestamps predating the subscription). The
recorder must therefore **de-duplicate trades on `(coin, time, tid)`** on
startup and after every reconnect rather than assume the first batch is new.

## 4. REST endpoints

All market data is `POST https://api.hyperliquid.xyz/info` with
`Content-Type: application/json` (testnet: `https://api.hyperliquid-testnet.xyz/info`).
The request type lives in the body; there are no query strings and no paths.
Source: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint>
and <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals>

### Asset metadata — `meta`

```json
{ "type": "meta", "dex": "" }
```

Live response (trimmed):

```json
{
  "universe": [
    {"szDecimals": 5, "name": "BTC", "maxLeverage": 40, "marginTableId": 56},
    {"szDecimals": 4, "name": "ETH", "maxLeverage": 25, "marginTableId": 55}
  ],
  "marginTables": [[50, {"description": "", "marginTiers": [{"lowerBound": "0.0", "maxLeverage": 50}]}]],
  "collateralToken": "USDC"
}
```

**The index of an asset in `universe` is its asset id** for every other API that
takes one. Order is significant and new listings are appended, so cache the list
with its order, keyed by name, and re-pull on a listing change rather than
assuming a fixed id.

Per-asset fields: `name`, `szDecimals`, `maxLeverage`, `marginTableId`, plus
`onlyIsolated: true` on isolated-only markets and `isDelisted` / `marginMode`
where applicable. There is **no tick-size field** — price precision is derived
(§7). This is the source of `szDecimals` for the whole system.

### Funding + OI for all assets — `metaAndAssetCtxs`

```json
{ "type": "metaAndAssetCtxs", "dex": "" }
```

Returns a two-element array `[meta, assetCtxs]`. `assetCtxs` is positionally
aligned with `meta.universe` — **there is no `coin` field in the context
objects**, so the join is by index. Getting that join wrong attributes BTC's
funding to ETH and produces plausible-looking garbage.

Live `assetCtxs[0]` (BTC), same field set as `activeAssetCtx.ctx`:

```json
{"funding": "0.0000020288", "openInterest": "38518.20404", "prevDayPx": "83367.0",
 "dayNtlVlm": "3307460952.327064991", "premium": "-0.000456303", "oraclePx": "83278.0",
 "markPx": "83240.0", "midPx": "83239.5", "impactPxs": ["83239.0", "83240.0"],
 "dayBaseVlm": "39963.8011200001"}
```

This single request covers both the `funding` and `open_interest` streams for
every perp, which is why it is the recorder's funding path at
`poll_seconds: 60`.

### Predicted funding — `predictedFundings`

```json
{ "type": "predictedFundings" }
```

Live, filtered to BTC:

```json
[["BTC", [
  ["BinPerp",   {"fundingRate": "0.00007048",   "nextFundingTime": 1791907200000, "fundingIntervalHours": 8}],
  ["HlPerp",    {"fundingRate": "0.0000035092", "nextFundingTime": 1791892800000, "fundingIntervalHours": 1}],
  ["BybitPerp", {"fundingRate": "0.00008991",   "nextFundingTime": 1791907200000, "fundingIntervalHours": 8}]
]]]
```

Array of `[coin, [[venue, {fundingRate, nextFundingTime, fundingIntervalHours}]]]`.
`fundingIntervalHours` is present live but not in the docs' example — do not
rely on it existing, but do read it when present rather than hardcoding 1.
Hyperliquid's own entry is `HlPerp`, and its `fundingRate` matched
`activeAssetCtx.funding` exactly in the capture: the predicted and current rate
are the *same* number, the running estimate for the hour in progress. First perp
dex only.

### Historical funding — `fundingHistory`

```json
{ "type": "fundingHistory", "coin": "BTC", "startTime": 1791800000000, "endTime": 1791893000000 }
```

`startTime` required (ms, inclusive), `endTime` optional (ms, inclusive,
defaults to now). Live response:

```json
[{"coin": "BTC", "fundingRate": "0.0000125", "premium": "-0.0003558005", "time": 1791802800098}]
```

One row per settled hour. 26 rows came back for a ~26 h window, confirming
hourly cadence. `time` is the settlement time, with a few tens of ms of block
jitter (`...800098`, `...400032`) — bucket to the hour before joining, do not
match on exact equality.

This is the backfill endpoint for gap repair on the `funding` stream, and the
only funding data that is *recoverable* after an outage. `l2` and `trades` are
not.

### Candles — `candleSnapshot`

```json
{ "type": "candleSnapshot",
  "req": { "coin": "BTC", "interval": "15m", "startTime": 1681923600000, "endTime": 1681924499999 } }
```

Note the nested `req` object, unlike every other info request.

```json
[{"t": 1681923600000, "T": 1681924499999, "s": "BTC", "i": "15m",
  "o": "29295.0", "c": "29258.0", "h": "29309.0", "l": "29250.0",
  "n": 189, "v": "0.98639"}]
```

`t`/`T` open/close time (ms), `s` coin, `i` interval, `o`/`h`/`l`/`c` prices
(strings), `v` volume in base units (string), `n` trade count (int). Intervals:
`1m 3m 5m 15m 30m 1h 2h 4h 8h 12h 1d 3d 1w 1M`. **Only the most recent 5000
candles are available** — this is not a historical archive, and at 1m that is
under four days. It cannot substitute for recording.

### Book and tape snapshots

`{"type": "l2Book", "coin": "BTC", "nSigFigs": 5, "mantissa": 2}` returns the
same `{coin, time, levels}` shape as the WS feed (`nSigFigs` ∈ {2,3,4,5,null};
`mantissa` ∈ {1,2,5} and only when `nSigFigs` is 5). `{"type": "recentTrades",
"coin": "BTC"}` returns an array of the same trade objects as the WS feed —
verified live, same fields including `side`, `tid`, `users`. Useful for
cold-start and for sanity-checking the WS tape, but it is not deep history.

**`TODO(verify)`** — how far back `recentTrades` reaches and how many rows it
caps at. Not documented on the info-endpoint page; it appears in the rate-limit
table so it exists, but no response-size bound is stated.

## 5. Rate limits

Source: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits>

**REST, per IP:** an aggregate weight budget of **1200 per minute.**

| Info request | Weight |
|---|---|
| `l2Book`, `allMids`, `clearinghouseState`, `orderStatus`, `spotClearinghouseState`, `exchangeStatus` | 2 |
| `userRole` | 60 |
| all other documented info requests | 20 |

Some endpoints add weight per returned page: `fundingHistory`, `recentTrades`,
`userFills`, `historicalOrders` and others add per 20 items returned;
`candleSnapshot` adds per 60 items. So a wide `fundingHistory` backfill is far
more expensive than its base weight of 20 suggests — page it.

Budget check for this recorder: `metaAndAssetCtxs` at weight 20 every 60 s is
20/min against 1200. Nothing to worry about; the backfill paths are where a
breach would come from.

**WebSocket, per IP:**

| Limit | Value |
|---|---|
| Connections | 10 |
| New connections | 30 / minute |
| Subscriptions | 1000 |
| Unique users across user-specific subscriptions | 10 |
| Messages sent to Hyperliquid | 2000 / minute, all connections |
| Inflight `post` messages | 100, all connections |

The 1000-subscription cap is the binding one for `mode: all_perps`: with ~190+
perps listed, `trades` + `l2Book` + `activeAssetCtx` per asset would approach
it. `allMids` is one subscription for everything, and funding comes from REST —
that is the design reason, not an accident of configuration.

The 30-new-connections-per-minute cap also constrains reconnect policy: a
backoff that retries aggressively across several sockets can lock itself out.
Exponential backoff with jitter, not a tight loop.

**On breach:** address-based (action) limits degrade to one request every 10
seconds once tripped. **`TODO(verify)`** — the documented consequence of
breaching the *IP-based* REST weight limit or the WS caps. The page states the
limits but not the failure mode (HTTP 429 vs. connection close vs. silent drop).
Code defensively: treat any non-200 and any unexpected close as a gap event.

## 6. Funding mechanics

Source: <https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding>

| Property | Value |
|---|---|
| Settlement interval | **hourly** |
| Quoted rate in `funding` / `fundingRate` | the **per-hour** rate |
| Formula output | an 8-hour rate; each hourly payment is 1/8 of it |
| Interest rate component | 0.01% per 8 h = 0.00125% per hour |
| Premium sampling | every 5 s, averaged over the hour |
| Clamp | interest − premium clamped to ±0.05% |
| Cap | total funding capped at 4% per hour |
| Notional basis | position size × **oracle** price (not mark) |

`F = P + clamp(interest_rate − P, −0.0005, 0.0005)`, where `P` is the average
premium index. Premium for standard perps is the impact-price difference divided
by the oracle price; the impact prices are average execution prices for an
asset-specific impact notional, which is why `impactPxs` is exposed.

**The number on the wire is already per-hour.** Verified: `fundingHistory`
returned `fundingRate: "0.0000125"` for BTC, which is exactly the 0.0001 8-hour
interest floor divided by 8 — the hourly figure, not the 8-hour one. So
`FundingSample.interval_s = 3600` and `rate` is stored exactly as received. Do
not divide by 8 anywhere; that has already happened. Annualisation is
`rate × 24 × 365`.

"Predicted" vs "current" on Hyperliquid is a weaker distinction than on venues
with 8-hour funding:

| | Where | Meaning |
|---|---|---|
| Current / predicted | `activeAssetCtx.ctx.funding`, `metaAndAssetCtxs[1][i].funding`, `predictedFundings` `HlPerp.fundingRate` | the running estimate for the hour in progress; all three carried the identical value live |
| Settled | `fundingHistory[].fundingRate` | the rate actually charged for a completed hour, with its settlement `time` |
| Next settlement | `predictedFundings` `HlPerp.nextFundingTime` | ms timestamp of the next payment |

Because the in-progress rate moves as the premium accumulates, `poll_seconds:
60` records a path, not 60 redundant copies of one number — and that path is
only obtainable by recording it. `fundingHistory` gives the settled endpoint
afterwards and nothing in between.

`predictedFundings` also returns Binance and Bybit rates with their own
`fundingIntervalHours: 8`. **Those are 8-hour rates.** Comparing them to
Hyperliquid's hourly figure without rescaling understates HL funding by 8× —
which is why `FundingSample` stores `interval_s` alongside `rate` rather than
normalising at write time.

## 7. Tick, lot and minimum order rules

Source: <https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/tick-and-lot-size>,
<https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses>

There is no `tickSize` field. Valid prices are defined by two rules that apply
**simultaneously** — a price must satisfy both:

1. At most **5 significant figures.**
2. At most **`MAX_DECIMALS − szDecimals`** decimal places, where `MAX_DECIMALS`
   is **6 for perps** and **8 for spot.**

Integer prices are always valid regardless of significant figures — this is the
exception that makes BTC at `83245` legal where 5 sig figs would otherwise cap
it. Documented examples (perps):

| Price | Valid? | Why |
|---|---|---|
| `1234.5` | yes | 5 sig figs |
| `1234.56` | no | 6 sig figs |
| `0.001234` | yes | 6 decimals, within `6 − 0` |
| `0.0012345` | no | 7 decimals |
| `0.01234`, szDecimals 1 | yes | 5 decimals ≤ `6 − 1` |
| `0.012345`, szDecimals 1 | no | 6 decimals > `6 − 1` |

Sizes are rounded to `szDecimals` from `meta.universe`: with `szDecimals = 3`,
`1.001` is valid and `1.0001` is not. BTC's live `szDecimals` is 5.

When signing, remove trailing zeros — `83245.0` and `83245` are different
strings and the signature covers the string.

**Minimum order value: $10** per order (`MinTradeNtl`: "Order must have minimum
value of $10."). Spot is 10 units of the quote token (`MinTradeSpotNtl`).

**`TODO(verify)`** — a third-party source claims the minimum notional has been
lowered to $1 for HIP-4 markets and that the official docs were never updated.
Unconfirmed from any Hyperliquid source. Irrelevant to the recorder; relevant
before sizing live orders.

## 8. Gotchas

Ordered by how much damage each one does to a 24/7 recorder.

**Silent liveness.** The 60 s server timeout is on inbound-to-venue traffic. A
socket where our ping loop is healthy but no market data arrives stays open and
reports no error. The only reliable detector is a per-stream last-message clock
(`max_silence_seconds: 90`), evaluated per subscription and not per connection:
a dead `l2Book` on a connection whose `trades` is flowing will not trip any
connection-level check.

**Resubscribe is the client's job.** Nothing is restored after a reconnect.
Every reconnect must replay the full subscription set, and the window between
disconnect and the last successful `subscriptionResponse` is a gap — write it as
one.

**Duplicates across reconnects are expected, not exceptional.** The docs state
missed data appears in the snapshot ack on reconnect, and the first `trades`
batch after subscribing carried timestamps predating the subscription. Dedupe
`trades` on `(coin, time, tid)` unconditionally. For `l2` and `mids`,
duplicates are harmless as extra samples but will show up as zero-interval
rows in cadence analysis — tolerate them in queries rather than filtering at
write time.

**`tid` is not a unique id.** 50 bits, documented as a hash of the two order
ids. Collisions are possible by construction. Never use it alone as a primary
key.

**`openInterest` is in coin units.** Multiply by a price to get notional. A
series that silently mixes the two is unusable and the error is invisible per
sample.

**No event timestamp on `allMids` or `activeAssetCtx`.** Both must fall back to
ingest time. Record that the fallback happened (a schema flag or a convention
that `ts_event_ns == ts_ingest_ns` means "no venue stamp") so research can tell
observed latency from missing data.

**Positional joins.** `metaAndAssetCtxs` joins by index, `l2Book.levels` by
index, `bbo`/`impactPxs` by index. None of them carry a label. Assert
`len(universe) == len(assetCtxs)` on every poll and treat a mismatch as fatal
rather than zipping the shorter one.

**Listing churn.** `mode: all_perps` means the universe changes under the
recorder: new perps are appended to `universe` (shifting nothing, but invalidating
any cached "number of assets"), and `isDelisted` appears on removed ones. Re-pull
`meta` on a schedule and emit an event on change; a new listing that nobody
subscribed to is a silent hole in coverage, not an error.

**`allMids` carries non-perp keys.** 1294 keys live, of which the plain-named
ones are the perps. Filter on membership in `meta.universe`, not on string
shape — `@`/`#` prefixes are a current convention, not a documented contract.

**`nSigFigs` is lossy and irreversible.** Default it to unset. A recorder that
runs for a month at `nSigFigs: 4` has a month of rounded books.

**`TODO(verify)` — ordering guarantees.** The docs make no statement about
message ordering, whether per-subscription messages can arrive out of order, or
whether `time` is monotonic within a stream. Live captures showed non-monotonic
`time` *within* a single `trades` array (…62595, …62714, …63203 was ascending,
but ordering across batches was not tested). Do not rely on arrival order;
sort by `time` at query time and keep arrival order as data.

**`TODO(verify)` — `candleSnapshot` 5000-candle cap semantics.** Documented as
"only the most recent 5000 candles"; whether an older `startTime` returns an
empty array, a truncated window, or an error is not stated.
