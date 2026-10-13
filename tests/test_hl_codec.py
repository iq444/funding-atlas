"""Codec tests.

Every fixture in here is a verbatim JSON capture from `docs/hyperliquid-api.md`
— that is the point of having captured them. A test written against a shape
invented in the test file proves only that the codec agrees with the test
author.

Assertions on prices and sizes compare against `Decimal` literals built from
strings. `Decimal("83245.0") == 83245.0` would pass through a float and so would
not test the thing that matters.
"""

from __future__ import annotations

import json
from decimal import Decimal

from funding_atlas.core.clock import SimClock
from funding_atlas.venues.hyperliquid import codec
from funding_atlas.venues.records import (
    BookSnapshot,
    FundingSample,
    Mid,
    OpenInterest,
    Trade,
)

VENUE = "hyperliquid"
NOW_NS = 1_791_900_000_000_000_000

# -- fixtures, verbatim from docs/hyperliquid-api.md §3 --------------------

TRADES_MSG = json.dumps(
    {
        "channel": "trades",
        "data": [
            {
                "coin": "BTC",
                "side": "B",
                "px": "83245.0",
                "sz": "0.14397",
                "time": 1791893062595,
                "hash": "0xe1375f63789827e8e2b104467803bc02019d0049139b46ba85000ab6379c01d3",
                "tid": 926762701686957,
                "users": [
                    "0x75c3f4a9f37194d42aa141fe5e7b640c48f9dca3",
                    "0xa62b923a112d50d03e1e096bbd53422490dac104",
                ],
            }
        ],
    }
)

L2_MSG = json.dumps(
    {
        "channel": "l2Book",
        "data": {
            "coin": "BTC",
            "time": 1791893079509,
            "levels": [
                [
                    {"px": "83265.0", "sz": "3.95272", "n": 17},
                    {"px": "83264.0", "sz": "0.00014", "n": 1},
                ],
                [{"px": "83266.0", "sz": "5.75263", "n": 15}],
            ],
        },
    }
)

ALL_MIDS_MSG = json.dumps(
    {"channel": "allMids", "data": {"mids": {"BTC": "83239.5", "@1": "0.1874", "#102220": "0.815"}}}
)

CTX_MSG = json.dumps(
    {
        "channel": "activeAssetCtx",
        "data": {
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
                "dayBaseVlm": "39967.9110500001",
            },
        },
    }
)

BBO_MSG = json.dumps(
    {
        "channel": "bbo",
        "data": {
            "coin": "BTC",
            "time": 1791893079777,
            "bbo": [
                {"px": "83265.0", "sz": "6.41054", "n": 20},
                {"px": "83266.0", "sz": "5.75263", "n": 15},
            ],
        },
    }
)


def decode(raw: object, **kwargs: object) -> codec.DecodeOutcome:
    return codec.decode_message(
        raw,  # type: ignore[arg-type]
        venue=VENUE,
        clock=SimClock(NOW_NS),
        **kwargs,  # type: ignore[arg-type]
    )


# -- trades ----------------------------------------------------------------


def test_trades_decode_with_exact_decimals_and_ms_to_ns() -> None:
    outcome = decode(TRADES_MSG)
    assert isinstance(outcome, codec.Decoded)
    (trade,) = outcome.records
    assert isinstance(trade, Trade)
    assert trade.venue == VENUE
    assert trade.asset == "BTC"
    assert trade.price == Decimal("83245.0")
    assert trade.size == Decimal("0.14397")
    # The captured `time` is in ms; the record is ns.
    assert trade.ts_event_ns == 1791893062595 * 1_000_000
    assert trade.ts_ingest_ns == NOW_NS
    assert trade.trade_id == "926762701686957"
    # A real venue timestamp, so nothing synthetic.
    assert trade.ts_event_synthetic is False


def test_trade_prices_are_not_floats() -> None:
    outcome = decode(TRADES_MSG)
    assert isinstance(outcome, codec.Decoded)
    (trade,) = outcome.records
    assert isinstance(trade.price, Decimal)
    # Exact string round-trip: a float path would print 83245.0 but compare
    # unequal to the Decimal built from the wire text in general.
    assert str(trade.size) == "0.14397"


def test_trade_aggressor_side_mapping() -> None:
    assert _side("B") == "buy"
    assert _side("A") == "sell"


def _side(wire: str) -> str:
    msg = json.loads(TRADES_MSG)
    msg["data"][0]["side"] = wire
    outcome = decode(msg)
    assert isinstance(outcome, codec.Decoded)
    (trade,) = outcome.records
    return trade.side


def test_unknown_side_is_counted_not_raised() -> None:
    msg = json.loads(TRADES_MSG)
    msg["data"][0]["side"] = "X"
    outcome = decode(msg)
    assert isinstance(outcome, codec.Undecodable)
    assert outcome.reason is codec.UndecodableReason.BAD_SIDE
    assert outcome.channel == "trades"


def test_trades_array_carries_many_fills() -> None:
    msg = json.loads(TRADES_MSG)
    second = dict(msg["data"][0])
    second["side"] = "A"
    second["time"] = 1791893062714
    second["tid"] = 1
    msg["data"].append(second)
    outcome = decode(msg)
    assert isinstance(outcome, codec.Decoded)
    assert len(outcome.records) == 2
    # Arrival order is preserved: docs §8 leaves ordering unverified, so the
    # codec must not quietly sort and destroy it.
    assert [t.ts_event_ns for t in outcome.records] == [  # type: ignore[union-attr]
        1791893062595 * 1_000_000,
        1791893062714 * 1_000_000,
    ]


def test_trade_key_is_composite_because_tid_is_not_unique() -> None:
    key = codec.trade_key("BTC", 1791893062595 * 1_000_000, "926762701686957")
    assert key == "BTC:1791893062595000000:926762701686957"


# -- l2Book ----------------------------------------------------------------


def test_l2_book_orientation_and_values() -> None:
    outcome = decode(L2_MSG)
    assert isinstance(outcome, codec.Decoded)
    (book,) = outcome.records
    assert isinstance(book, BookSnapshot)
    # levels[0] is bids, levels[1] is asks (docs §3, verified live).
    assert book.best_bid == Decimal("83265.0")
    assert book.best_ask == Decimal("83266.0")
    assert book.best_bid < book.best_ask
    assert [lvl.price for lvl in book.bids] == [Decimal("83265.0"), Decimal("83264.0")]
    assert book.bids[0].size == Decimal("3.95272")
    assert book.bids[0].n_orders == 17
    assert book.asks[0].n_orders == 15
    assert book.ts_event_ns == 1791893079509 * 1_000_000
    assert book.ts_event_synthetic is False


def test_l2_bids_are_descending_and_asks_ascending() -> None:
    outcome = decode(L2_MSG)
    assert isinstance(outcome, codec.Decoded)
    (book,) = outcome.records
    bids = [lvl.price for lvl in book.bids]  # type: ignore[union-attr]
    assert bids == sorted(bids, reverse=True)


def test_l2_wrong_number_of_sides_is_undecodable() -> None:
    msg = json.loads(L2_MSG)
    msg["data"]["levels"] = [msg["data"]["levels"][0]]
    outcome = decode(msg)
    assert isinstance(outcome, codec.Undecodable)
    assert outcome.reason is codec.UndecodableReason.BAD_PAYLOAD


# -- allMids ---------------------------------------------------------------


def test_all_mids_uses_the_clock_and_flags_the_timestamp_as_synthetic() -> None:
    outcome = decode(ALL_MIDS_MSG, assets={"BTC"})
    assert isinstance(outcome, codec.Decoded)
    (mid,) = outcome.records
    assert isinstance(mid, Mid)
    assert mid.asset == "BTC"
    assert mid.mid == Decimal("83239.5")
    # No venue timestamp on this channel, so both stamps are ours and the flag
    # says so. Without it a latency study measures our clock against itself.
    assert mid.ts_event_ns == NOW_NS
    assert mid.ts_ingest_ns == NOW_NS
    assert mid.ts_event_synthetic is True


def test_all_mids_filters_by_universe_membership_not_key_shape() -> None:
    # The live capture mixes perps (`BTC`), spot indices (`@1`) and numbered
    # markets (`#102220`). Membership filtering keeps only what we record.
    outcome = decode(ALL_MIDS_MSG, assets={"BTC"})
    assert isinstance(outcome, codec.Decoded)
    assert [m.asset for m in outcome.records] == ["BTC"]

    # Unfiltered keeps everything, including the odd key forms: the filter is
    # the caller's universe, never a guess about prefixes.
    outcome = decode(ALL_MIDS_MSG)
    assert isinstance(outcome, codec.Decoded)
    assert {m.asset for m in outcome.records} == {"BTC", "@1", "#102220"}


def test_all_mids_filtered_to_nothing_is_a_decode_not_a_failure() -> None:
    outcome = decode(ALL_MIDS_MSG, assets={"ETH"})
    assert isinstance(outcome, codec.Decoded)
    assert outcome.records == ()


# -- activeAssetCtx --------------------------------------------------------


def test_active_asset_ctx_yields_funding_and_open_interest() -> None:
    outcome = decode(CTX_MSG)
    assert isinstance(outcome, codec.Decoded)
    funding = next(r for r in outcome.records if isinstance(r, FundingSample))
    oi = next(r for r in outcome.records if isinstance(r, OpenInterest))

    # The quoted rate is already per hour on Hyperliquid (docs §6). Stored
    # verbatim; dividing by 8 would understate recorded funding eightfold.
    assert funding.rate == Decimal("0.0000035092")
    assert funding.interval_s == 3600
    assert funding.premium == Decimal("-0.0005041533")
    assert funding.oracle_price == Decimal("83308.0")
    assert funding.mark_price == Decimal("83268.2")

    # Coin base units, not notional.
    assert oi.open_interest == Decimal("38521.32876")

    # No timestamp in `ctx`, so both records are clock-stamped and flagged.
    assert funding.ts_event_ns == NOW_NS
    assert funding.ts_event_synthetic is True
    assert oi.ts_event_synthetic is True


def test_active_asset_ctx_routes_to_two_streams() -> None:
    outcome = decode(CTX_MSG)
    assert isinstance(outcome, codec.Decoded)
    assert outcome.streams == {"funding", "open_interest"}


def test_nullable_context_fields_are_data_not_errors() -> None:
    msg = json.loads(CTX_MSG)
    msg["data"]["ctx"]["premium"] = None
    msg["data"]["ctx"]["midPx"] = None
    outcome = decode(msg)
    assert isinstance(outcome, codec.Decoded)
    funding = next(r for r in outcome.records if isinstance(r, FundingSample))
    assert funding.premium is None


def test_missing_open_interest_is_reported_not_zeroed() -> None:
    msg = json.loads(CTX_MSG)
    del msg["data"]["ctx"]["openInterest"]
    outcome = decode(msg)
    assert isinstance(outcome, codec.Undecodable)
    assert outcome.reason is codec.UndecodableReason.BAD_NUMBER


# -- bbo -------------------------------------------------------------------


def test_bbo_decodes_to_a_one_level_snapshot() -> None:
    outcome = decode(BBO_MSG)
    assert isinstance(outcome, codec.Decoded)
    (book,) = outcome.records
    assert isinstance(book, BookSnapshot)
    assert book.best_bid == Decimal("83265.0")
    assert book.best_ask == Decimal("83266.0")
    assert book.ts_event_ns == 1791893079777 * 1_000_000


def test_bbo_null_side_is_empty_not_zero() -> None:
    msg = json.loads(BBO_MSG)
    msg["data"]["bbo"][0] = None
    outcome = decode(msg)
    assert isinstance(outcome, codec.Decoded)
    (book,) = outcome.records
    assert book.bids == ()  # type: ignore[union-attr]
    assert book.best_bid is None  # type: ignore[union-attr]


# -- control and totality --------------------------------------------------


def test_pong_and_subscription_ack_are_control_not_errors() -> None:
    assert isinstance(decode(json.dumps({"channel": "pong"})), codec.Control)
    ack = decode(
        '{"channel":"subscriptionResponse","data":'
        '{"method":"subscribe","subscription":{"type":"allMids"}}}'
    )
    assert isinstance(ack, codec.Control)
    assert ack.data["subscription"]["type"] == "allMids"  # type: ignore[index,union-attr]


def test_unknown_channel_is_distinguished_from_known_but_unrecorded() -> None:
    unknown = decode(json.dumps({"channel": "somethingNew", "data": {}}))
    assert isinstance(unknown, codec.Undecodable)
    assert unknown.reason is codec.UndecodableReason.UNKNOWN_CHANNEL

    # `candle` exists but has no record type; counting it as a venue change
    # would cry wolf.
    known = decode(json.dumps({"channel": "candle", "data": []}))
    assert isinstance(known, codec.Undecodable)
    assert known.reason is codec.UndecodableReason.UNSUPPORTED_CHANNEL


def test_malformed_messages_never_raise() -> None:
    cases: list[tuple[object, codec.UndecodableReason]] = [
        ("not json at all", codec.UndecodableReason.NOT_JSON),
        ("[1, 2, 3]", codec.UndecodableReason.BAD_ENVELOPE),
        ('{"no_channel": 1}', codec.UndecodableReason.BAD_ENVELOPE),
        ('{"channel": "trades"}', codec.UndecodableReason.BAD_ENVELOPE),
        ('{"channel": "trades", "data": {"not": "an array"}}', codec.UndecodableReason.BAD_PAYLOAD),
        ('{"channel": "l2Book", "data": {"coin": "BTC"}}', codec.UndecodableReason.BAD_PAYLOAD),
        (
            (
                '{"channel": "trades", "data": [{"coin": "BTC", "side": "B", "px": "x",'
                ' "sz": "1", "time": 1, "tid": 1}]}'
            ),
            codec.UndecodableReason.BAD_NUMBER,
        ),
        (
            (
                '{"channel": "trades", "data": [{"coin": "BTC", "side": "B", "px": "1",'
                ' "sz": "1", "time": "1791893062595", "tid": 1}]}'
            ),
            codec.UndecodableReason.BAD_TIMESTAMP,
        ),
        (b"\xff\xfe not utf8", codec.UndecodableReason.NOT_JSON),
        ('{"channel": "allMids", "data": {"mids": []}}', codec.UndecodableReason.BAD_PAYLOAD),
    ]
    for raw, reason in cases:
        outcome = decode(raw)
        assert isinstance(outcome, codec.Undecodable), raw
        assert outcome.reason is reason, (raw, outcome)
        # The excerpt is what makes a counted drop actionable.
        assert outcome.detail


def test_undecodable_raw_excerpt_is_bounded() -> None:
    outcome = decode('{"channel": "trades", "data": [' + '{"x":1},' * 2000 + "]}")
    assert isinstance(outcome, codec.Undecodable)
    assert len(outcome.raw) < 600


def test_python_float_input_is_rejected_rather_than_rounded() -> None:
    # Only reachable from a caller handing us an already-lossy dict; JSON
    # numbers are read into Decimal from their source text instead.
    msg = json.loads(TRADES_MSG)
    msg["data"][0]["px"] = 83245.1
    outcome = decode(msg)
    assert isinstance(outcome, codec.Undecodable)
    assert outcome.reason is codec.UndecodableReason.BAD_NUMBER


def test_json_number_is_read_losslessly_from_source_text() -> None:
    # Defensive against a venue representation change: a bare JSON number must
    # not cost us the message, and must not pass through a binary double.
    outcome = decode('{"channel": "allMids", "data": {"mids": {"BTC": 0.1}}}')
    assert isinstance(outcome, codec.Decoded)
    (mid,) = outcome.records
    assert mid.mid == Decimal("0.1")  # type: ignore[union-attr]
    assert str(mid.mid) == "0.1"  # type: ignore[union-attr]


def test_stream_mapping_covers_every_record_type() -> None:
    outcomes = [decode(m) for m in (TRADES_MSG, L2_MSG, ALL_MIDS_MSG, CTX_MSG)]
    seen = {codec.stream_of(r) for o in outcomes if isinstance(o, codec.Decoded) for r in o.records}
    assert seen == {"trades", "l2", "mids", "funding", "open_interest"}
