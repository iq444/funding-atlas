"""Hyperliquid WebSocket wire format -> `venues/records.py`.

Pure functions, no IO and no sockets, so the whole wire contract is testable
from the JSON captures in `docs/hyperliquid-api.md`. That document is the
authority for every field name and unit used here; where it carries a
`TODO(verify)` the code below is written to survive either answer and says so.

Three invariants from that document drive the design:

* **Every numeric on the wire is a JSON string.** They are fed straight to
  `Decimal`. A float is never constructed, not even transiently: `0.1` as a
  binary double is not `0.1`, and a book recorded through one is wrong in a way
  no later processing can undo. A number that arrives as a JSON *float* is
  therefore reported as undecodable rather than quietly rounded — if the venue
  ever changes representation we want to find out from a counter, not from a
  research result.

* **Every venue timestamp is milliseconds.** `records.py` stores ns, so the
  conversion is `* 1_000_000`. The three trailing zeros are not precision.

* **Some streams carry no time at all** (`allMids`, `activeAssetCtx`). Those
  take `ts_event_ns` from the injected clock and set `ts_event_synthetic`, which
  is the only thing that lets research tell a measured latency from a fabricated
  one.

Decoding is **total**. Nothing in here raises at the caller: every input maps to
either a `Decoded`, a `Control` or an `Undecodable`. A read loop that dies on one
odd message loses every stream on that socket, and one that silently swallows it
hides a venue change for as long as nobody looks. `Undecodable` carries a reason
from a small taxonomy precisely so the caller can count and log both cases.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import StrEnum
from typing import Any, Final

from funding_atlas.core.clock import Clock
from funding_atlas.venues.records import (
    BookLevel,
    BookSnapshot,
    FundingSample,
    Mid,
    OpenInterest,
    Side,
    Trade,
)

__all__ = [
    "CONTROL_CHANNELS",
    "DATA_CHANNELS",
    "MS_TO_NS",
    "STREAM_BY_RECORD",
    "Control",
    "DecodeOutcome",
    "Decoded",
    "Record",
    "Undecodable",
    "UndecodableReason",
    "decode_active_asset_ctx",
    "decode_all_mids",
    "decode_bbo",
    "decode_l2_book",
    "decode_message",
    "decode_trades",
    "stream_of",
    "trade_key",
]

#: Venue timestamps are milliseconds everywhere in this API (docs §intro).
MS_TO_NS: Final = 1_000_000

#: Hyperliquid's aggressor encoding. "B — Bid, i.e. Buy", "A — Ask, i.e. Short",
#: and "Side is aggressing side for trades" (docs §3 trades, verified live: a
#: `B` printed at the ask and an `A` at the bid, i.e. the taker's side).
SIDE_BY_WIRE: Final[dict[str, Side]] = {"B": "buy", "A": "sell"}

#: Only `levels[0]`/`levels[1]`, never a named object. Confirmed against a live
#: mainnet response, not documented — hence the constant with the citation.
_BIDS: Final = 0
_ASKS: Final = 1

Record = Trade | Mid | BookSnapshot | FundingSample | OpenInterest

#: Record type -> `records.STREAMS` key. One venue message can produce records of
#: two different streams (`activeAssetCtx` yields funding *and* open interest),
#: so routing is per record, never per channel.
STREAM_BY_RECORD: Final[dict[type, str]] = {
    Trade: "trades",
    Mid: "mids",
    BookSnapshot: "l2",
    FundingSample: "funding",
    OpenInterest: "open_interest",
}

#: Channels that are protocol bookkeeping rather than market data. They are
#: named rather than lumped in with the unknown ones: a `pong` counted as a
#: venue change would cry wolf every 50 seconds.
CONTROL_CHANNELS: Final[frozenset[str]] = frozenset({"pong", "subscriptionResponse", "post"})

#: Channels this codec turns into records. `candle`, `activeSpotAssetCtx` and the
#: user-specific channels are deliberately absent: there is no record type for
#: them, and treating them as unsupported-but-known keeps the unknown-channel
#: counter meaningful as a venue-change alarm.
DATA_CHANNELS: Final[frozenset[str]] = frozenset(
    {"trades", "l2Book", "allMids", "activeAssetCtx", "bbo"}
)

#: Known to the venue, not recorded by us. Counted separately from genuinely
#: unknown channels.
_UNSUPPORTED_CHANNELS: Final[frozenset[str]] = frozenset(
    {
        "candle",
        "activeSpotAssetCtx",
        "notification",
        "webData2",
        "orderUpdates",
        "user",
        "userFills",
        "userFundings",
        "userNonFundingLedgerUpdates",
        "userTwapSliceFills",
        "userTwapHistory",
    }
)

#: A raw message kept whole in an error payload can be 20 book levels wide and
#: lands in the event log once per bad message. Truncate it there, not here.
_RAW_EXCERPT_CHARS: Final = 512


class UndecodableReason(StrEnum):
    """Why a message produced no records.

    Split finely enough that the counters answer different questions:
    `UNKNOWN_CHANNEL` means the venue is sending something new, `BAD_FIELD`
    means a field we rely on changed shape, `NOT_JSON` usually means the
    transport, and `INTERNAL` means this codec has a bug. Collapsing them into
    one "dropped" counter would make all four look like the same incident.
    """

    NOT_JSON = "not_json"
    BAD_ENVELOPE = "bad_envelope"
    UNKNOWN_CHANNEL = "unknown_channel"
    UNSUPPORTED_CHANNEL = "unsupported_channel"
    BAD_PAYLOAD = "bad_payload"
    BAD_FIELD = "bad_field"
    BAD_NUMBER = "bad_number"
    BAD_TIMESTAMP = "bad_timestamp"
    BAD_SIDE = "bad_side"
    INTERNAL = "internal"


@dataclass(frozen=True, slots=True)
class Decoded:
    """Records extracted from one venue message.

    `records` can be empty on a well-formed message — an `allMids` map filtered
    down to nothing, or a `bbo` with both sides null. That is a successful decode
    of a message with no content, which is a different fact from a failure and
    is kept distinguishable from one.
    """

    channel: str
    records: tuple[Record, ...] = ()

    @property
    def streams(self) -> frozenset[str]:
        return frozenset(stream_of(r) for r in self.records)


@dataclass(frozen=True, slots=True)
class Control:
    """Protocol traffic: `pong`, subscription acks, `post` replies.

    Carries `data` because a `subscriptionResponse` is the only confirmation that
    a resubscribe landed, and the reconnect path has to be able to check it.
    """

    channel: str
    data: Any = None


@dataclass(frozen=True, slots=True)
class Undecodable:
    """A message that produced nothing, with enough detail to act on it."""

    reason: UndecodableReason
    detail: str
    channel: str | None = None
    #: Truncated excerpt, for the log line. Never the parsed object: by
    #: definition we do not trust its shape.
    raw: str = field(default="", repr=False)


DecodeOutcome = Decoded | Control | Undecodable


def stream_of(record: Record) -> str:
    """`records.STREAMS` key for a decoded record."""
    try:
        return STREAM_BY_RECORD[type(record)]
    except KeyError as exc:  # pragma: no cover - guards a future record type
        raise TypeError(f"no stream mapping for {type(record).__name__}") from exc


def trade_key(asset: str, ts_event_ns: int, trade_id: str) -> str:
    """Dedupe key for trades.

    `tid` is a 50-bit hash of the two order ids (docs §3), so it is not unique on
    its own and collisions exist by construction. Worse, the first `trades` batch
    after a (re)subscribe arrived with timestamps predating the subscription —
    docs §3 "Snapshot flag" `TODO(verify)` leaves it open whether that is a
    documented snapshot or incidental. Either way duplicates across reconnects
    are expected, so the recorder dedupes unconditionally on this key rather than
    trusting any `isSnapshot` flag to appear.
    """
    return f"{asset}:{ts_event_ns}:{trade_id}"


class _Malformed(Exception):
    """Internal: unwound at the channel boundary into an `Undecodable`.

    Private so that no exception from this module can reach a read loop.
    """

    def __init__(self, reason: UndecodableReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


# -- field parsing ---------------------------------------------------------


def _dec(value: Any, what: str) -> Decimal:
    """Parse a venue numeric. Strings on the wire, by contract (docs §intro).

    A Python `float` reaching here is rejected rather than accepted-and-rounded:
    its precision is already gone, and `Decimal(0.1)` would record
    `0.1000000000000000055511151231257827` as if the venue had said it. JSON
    numbers do *not* arrive as floats — `_parse` reads them straight into
    `Decimal` from the source text — so this branch only fires for a caller that
    hands us an already-lossy dict.
    """
    if isinstance(value, (bool, float)):
        raise _Malformed(
            UndecodableReason.BAD_NUMBER,
            f"{what} is {type(value).__name__} {value!r}; the venue sends numerics as strings "
            "and parsing through float would lose precision irrecoverably",
        )
    if not isinstance(value, (str, int, Decimal)):
        raise _Malformed(
            UndecodableReason.BAD_NUMBER, f"{what} is {type(value).__name__}, expected string"
        )
    try:
        return Decimal(value)
    except (InvalidOperation, ValueError) as exc:
        raise _Malformed(UndecodableReason.BAD_NUMBER, f"{what}={value!r} is not a number") from exc


def _ts_ns(value: Any, what: str) -> int:
    """Venue millisecond timestamp -> ns."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise _Malformed(
            UndecodableReason.BAD_TIMESTAMP,
            f"{what} is {type(value).__name__}, expected int milliseconds",
        )
    if value <= 0:
        raise _Malformed(UndecodableReason.BAD_TIMESTAMP, f"{what}={value} is not a valid ms epoch")
    return value * MS_TO_NS


def _text(obj: Mapping[str, Any], key: str, what: str) -> str:
    value = obj.get(key)
    if not isinstance(value, str) or not value:
        raise _Malformed(UndecodableReason.BAD_FIELD, f"{what}.{key}={value!r} is not a string")
    return value


def _n_orders(value: Any) -> int | None:
    # `n` is documented as the order count at a level. Optional in our record, so
    # an unexpected type degrades the level rather than discarding the snapshot —
    # losing a count is survivable, losing the book is not.
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _mapping(value: Any, what: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _Malformed(
            UndecodableReason.BAD_PAYLOAD, f"{what} is {type(value).__name__}, expected object"
        )
    return value


def _sequence(value: Any, what: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise _Malformed(
            UndecodableReason.BAD_PAYLOAD, f"{what} is {type(value).__name__}, expected array"
        )
    return value


def _level(raw: Any, what: str) -> BookLevel:
    level = _mapping(raw, what)
    return BookLevel(
        price=_dec(level.get("px"), f"{what}.px"),
        size=_dec(level.get("sz"), f"{what}.sz"),
        n_orders=_n_orders(level.get("n")),
    )


def _levels(raw: Any, what: str) -> tuple[BookLevel, ...]:
    return tuple(_level(entry, f"{what}[{i}]") for i, entry in enumerate(_sequence(raw, what)))


# -- per-channel decoders --------------------------------------------------


def decode_trades(
    data: Any,
    *,
    venue: str,
    clock: Clock,
    assets: Collection[str] | None = None,
) -> tuple[Trade, ...]:
    """`{"channel":"trades","data":[{...},...]}`.

    `data` is an **array**: one message carries a whole block's fills. Order
    within the array is not guaranteed monotonic (docs §8 `TODO(verify)` on
    ordering), so arrival order is preserved as data and never relied on — we do
    not sort here, because arrival order is itself a recorded fact.
    """
    ts_ingest_ns = clock.now_ns()
    out: list[Trade] = []
    for i, raw in enumerate(_sequence(data, "trades")):
        entry = _mapping(raw, f"trades[{i}]")
        asset = _text(entry, "coin", f"trades[{i}]")
        if assets is not None and asset not in assets:
            continue
        wire_side = entry.get("side")
        side = SIDE_BY_WIRE.get(wire_side) if isinstance(wire_side, str) else None
        if side is None:
            raise _Malformed(
                UndecodableReason.BAD_SIDE,
                f"trades[{i}].side={wire_side!r}, expected 'B' (buy aggressor) or 'A' (sell)",
            )
        ts_event_ns = _ts_ns(entry.get("time"), f"trades[{i}].time")
        tid = entry.get("tid")
        if isinstance(tid, bool) or not isinstance(tid, (int, str)):
            raise _Malformed(
                UndecodableReason.BAD_FIELD,
                f"trades[{i}].tid={tid!r} is not an int or string",
            )
        out.append(
            Trade(
                venue=venue,
                asset=asset,
                ts_event_ns=ts_event_ns,
                ts_ingest_ns=ts_ingest_ns,
                price=_dec(entry.get("px"), f"trades[{i}].px"),
                size=_dec(entry.get("sz"), f"trades[{i}].sz"),
                side=side,
                # Stored as the bare venue id. The composite dedupe key is built
                # by `trade_key` where it is needed, so the record keeps exactly
                # what the venue said and nothing synthesised.
                trade_id=str(tid),
            )
        )
    return tuple(out)


def decode_l2_book(
    data: Any,
    *,
    venue: str,
    clock: Clock,
    assets: Collection[str] | None = None,
) -> tuple[BookSnapshot, ...]:
    """`{"channel":"l2Book","data":{"coin","time","levels":[[bids],[asks]]}}`.

    Each message is a full top-20 snapshot of both sides, never a delta, so a
    dropped message costs one sample and cannot corrupt the next one. There is
    nothing incremental to apply.
    """
    book = _mapping(data, "l2Book")
    asset = _text(book, "coin", "l2Book")
    if assets is not None and asset not in assets:
        return ()
    sides = _sequence(book.get("levels"), "l2Book.levels")
    if len(sides) != 2:
        raise _Malformed(
            UndecodableReason.BAD_PAYLOAD,
            f"l2Book.levels has {len(sides)} sides, expected 2 ([bids, asks])",
        )
    return (
        BookSnapshot(
            venue=venue,
            asset=asset,
            ts_event_ns=_ts_ns(book.get("time"), "l2Book.time"),
            ts_ingest_ns=clock.now_ns(),
            bids=_levels(sides[_BIDS], "l2Book.levels[0]"),
            asks=_levels(sides[_ASKS], "l2Book.levels[1]"),
        ),
    )


def decode_bbo(
    data: Any,
    *,
    venue: str,
    clock: Clock,
    assets: Collection[str] | None = None,
) -> tuple[BookSnapshot, ...]:
    """`{"channel":"bbo","data":{"coin","time","bbo":[bid, ask]}}`.

    A one-level `BookSnapshot`. Positional like `l2Book.levels`, and either
    element may be null — a side with no quote becomes an empty tuple rather
    than a zero-priced level, because a zero bid and no bid are not the same
    market.
    """
    quote = _mapping(data, "bbo")
    asset = _text(quote, "coin", "bbo")
    if assets is not None and asset not in assets:
        return ()
    pair = _sequence(quote.get("bbo"), "bbo.bbo")
    if len(pair) != 2:
        raise _Malformed(
            UndecodableReason.BAD_PAYLOAD,
            f"bbo.bbo has {len(pair)} entries, expected 2 ([bid, ask])",
        )
    bid, ask = pair[_BIDS], pair[_ASKS]
    return (
        BookSnapshot(
            venue=venue,
            asset=asset,
            ts_event_ns=_ts_ns(quote.get("time"), "bbo.time"),
            ts_ingest_ns=clock.now_ns(),
            bids=() if bid is None else (_level(bid, "bbo.bbo[0]"),),
            asks=() if ask is None else (_level(ask, "bbo.bbo[1]"),),
        ),
    )


def decode_all_mids(
    data: Any,
    *,
    venue: str,
    clock: Clock,
    assets: Collection[str] | None = None,
) -> tuple[Mid, ...]:
    """`{"channel":"allMids","data":{"mids":{"BTC":"83239.5", ...}}}`.

    No venue timestamp, so `ts_event_ns` comes from our clock with
    `ts_event_synthetic=True` — the flag is what stops a later latency study
    from measuring our own ingest jitter against itself.

    1294 keys arrived live in three naming classes (`BTC`, `@1`, `#102220`).
    Filtering is by membership in `assets` (the venue's own `meta.universe`),
    never by the `@`/`#` prefix shape: those prefixes are a current convention,
    not a documented contract, and a shape-based filter would start dropping
    perps the day the convention changes.
    """
    payload = _mapping(data, "allMids")
    mids = _mapping(payload.get("mids"), "allMids.mids")
    now_ns = clock.now_ns()
    out: list[Mid] = []
    for asset, raw_px in mids.items():
        if not isinstance(asset, str):
            raise _Malformed(
                UndecodableReason.BAD_FIELD, f"allMids.mids key {asset!r} is not a string"
            )
        if assets is not None and asset not in assets:
            continue
        out.append(
            Mid(
                venue=venue,
                asset=asset,
                ts_event_ns=now_ns,
                ts_ingest_ns=now_ns,
                mid=_dec(raw_px, f"allMids.mids[{asset}]"),
                ts_event_synthetic=True,
            )
        )
    return tuple(out)


def decode_active_asset_ctx(
    data: Any,
    *,
    venue: str,
    clock: Clock,
    assets: Collection[str] | None = None,
) -> tuple[Record, ...]:
    """`{"channel":"activeAssetCtx","data":{"coin","ctx":{...}}}` -> funding + OI.

    Two records from one message, which is why routing is per record type.

    `funding` is Hyperliquid's **per-hour** rate, already divided: docs §6
    verified `fundingRate: "0.0000125"` against the 0.0001 8-hour interest floor
    / 8. So `interval_s = 3600` and the rate is stored exactly as received.
    Dividing by 8 here — the reflex from 8-hour venues — would understate
    recorded funding eightfold and look plausible doing it.

    `openInterest` is in **coin base units**, not USD notional. `OpenInterest`
    stores it verbatim; the mark price needed to convert arrives in this same
    message, so a consumer can do it without a second lookup.

    `ctx` has no timestamp, so both records are synthetic-timestamped, same as
    `allMids`.
    """
    envelope = _mapping(data, "activeAssetCtx")
    asset = _text(envelope, "coin", "activeAssetCtx")
    if assets is not None and asset not in assets:
        return ()
    ctx = _mapping(envelope.get("ctx"), "activeAssetCtx.ctx")
    now_ns = clock.now_ns()

    def optional(key: str) -> Decimal | None:
        # `midPx`, `premium` and `impactPxs` are documented nullable (HIP-3
        # markets), so null is normal data here and not a decode failure.
        raw = ctx.get(key)
        return None if raw is None else _dec(raw, f"activeAssetCtx.ctx.{key}")

    out: list[Record] = [
        FundingSample(
            venue=venue,
            asset=asset,
            ts_event_ns=now_ns,
            ts_ingest_ns=now_ns,
            rate=_dec(ctx.get("funding"), "activeAssetCtx.ctx.funding"),
            interval_s=3600,
            premium=optional("premium"),
            oracle_price=optional("oraclePx"),
            mark_price=optional("markPx"),
            ts_event_synthetic=True,
        )
    ]
    # Spot contexts (`activeSpotAssetCtx`) carry `circulatingSupply` instead and
    # are routed away before this point, so a missing `openInterest` here is a
    # perp context that changed shape — report it rather than writing a zero.
    out.append(
        OpenInterest(
            venue=venue,
            asset=asset,
            ts_event_ns=now_ns,
            ts_ingest_ns=now_ns,
            open_interest=_dec(ctx.get("openInterest"), "activeAssetCtx.ctx.openInterest"),
            ts_event_synthetic=True,
        )
    )
    return tuple(out)


_DECODERS: Final[dict[str, Any]] = {
    "trades": decode_trades,
    "l2Book": decode_l2_book,
    "allMids": decode_all_mids,
    "activeAssetCtx": decode_active_asset_ctx,
    "bbo": decode_bbo,
}
# Keeps the dispatch table and the advertised vocabulary from drifting apart.
assert frozenset(_DECODERS) == DATA_CHANNELS


# -- entry point -----------------------------------------------------------


def decode_message(
    raw: str | bytes | bytearray | Mapping[str, Any],
    *,
    venue: str,
    clock: Clock,
    assets: Collection[str] | None = None,
) -> DecodeOutcome:
    """Decode one inbound WS message. Never raises.

    `assets` is the recorded universe; `None` means "keep everything" and leaves
    filtering to the caller. Passing the venue's `meta.universe` here is what
    keeps `allMids`' 1294 keys from becoming 1294 recorded series.
    """
    excerpt = _excerpt(raw)
    try:
        payload = _parse(raw)
    except _Malformed as exc:
        return Undecodable(reason=exc.reason, detail=exc.detail, raw=excerpt)

    channel = payload.get("channel")
    if not isinstance(channel, str) or not channel:
        return Undecodable(
            reason=UndecodableReason.BAD_ENVELOPE,
            detail=f"channel={channel!r} is not a non-empty string",
            raw=excerpt,
        )

    if channel in CONTROL_CHANNELS:
        return Control(channel=channel, data=payload.get("data"))

    decoder = _DECODERS.get(channel)
    if decoder is None:
        known = channel in _UNSUPPORTED_CHANNELS
        return Undecodable(
            reason=(
                UndecodableReason.UNSUPPORTED_CHANNEL
                if known
                else UndecodableReason.UNKNOWN_CHANNEL
            ),
            detail=(
                f"channel {channel!r} is known but not recorded"
                if known
                else f"channel {channel!r} is not in this codec's vocabulary; "
                "the venue may have changed"
            ),
            channel=channel,
            raw=excerpt,
        )

    if "data" not in payload:
        return Undecodable(
            reason=UndecodableReason.BAD_ENVELOPE,
            detail=f"channel {channel!r} message has no 'data' field",
            channel=channel,
            raw=excerpt,
        )

    try:
        records = decoder(payload["data"], venue=venue, clock=clock, assets=assets)
    except _Malformed as exc:
        return Undecodable(reason=exc.reason, detail=exc.detail, channel=channel, raw=excerpt)
    except Exception as exc:  # noqa: BLE001 - totality is the point of this module
        # A bug in this codec must degrade to a counted drop, not take the read
        # loop (and with it every other subscription on the socket) down.
        return Undecodable(
            reason=UndecodableReason.INTERNAL,
            detail=f"{type(exc).__name__}: {exc}",
            channel=channel,
            raw=excerpt,
        )
    return Decoded(channel=channel, records=tuple(records))


def _parse(raw: str | bytes | bytearray | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(raw, Mapping):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise _Malformed(UndecodableReason.NOT_JSON, f"not utf-8: {exc}") from exc
    if not isinstance(raw, str):
        raise _Malformed(
            UndecodableReason.NOT_JSON, f"cannot decode a {type(raw).__name__} message"
        )
    try:
        # `parse_float=Decimal` is the whole float defence: a JSON number is
        # converted from its *source text*, so no binary double is ever built
        # even if the venue stops quoting numerics as strings one day. That is
        # deliberately lenient rather than a hard failure — a representation
        # change we can read losslessly should not cost us the data.
        parsed = json.loads(raw, parse_float=Decimal)
    except (json.JSONDecodeError, RecursionError) as exc:
        raise _Malformed(UndecodableReason.NOT_JSON, f"invalid JSON: {exc}") from exc
    if not isinstance(parsed, Mapping):
        raise _Malformed(
            UndecodableReason.BAD_ENVELOPE,
            f"top level is {type(parsed).__name__}, expected an object",
        )
    return parsed


def _excerpt(raw: Any) -> str:
    try:
        text = raw if isinstance(raw, str) else repr(raw)
    except Exception:  # noqa: BLE001 - a __repr__ must not break error reporting
        return "<unrepresentable>"
    return text if len(text) <= _RAW_EXCERPT_CHARS else text[:_RAW_EXCERPT_CHARS] + "...<truncated>"
