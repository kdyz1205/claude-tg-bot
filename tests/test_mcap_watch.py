"""Unit tests for trading.mcap_watch parsing and helpers."""

from __future__ import annotations

import pytest

from trading.mcap_watch import (
    ParsedWatchIntent,
    format_usd_compact,
    parse_cancel_intent,
    parse_list_intent,
    parse_mcap_watch_intent,
    parse_usd_amount,
    parsed_watch_from_dict,
)


def test_parse_usd_amount_mil():
    assert parse_usd_amount("12", "mil") == 12e6
    assert parse_usd_amount("10", "m") == 10e6
    assert parse_usd_amount("1.5", "million") == 1.5e6


def test_parse_mcap_watch_user_example():
    text = "当punch这个token 10mil 市值的币 当市值突破12mil 时发送提醒"
    p = parse_mcap_watch_intent(text)
    assert p is not None
    assert p.token_query.lower() == "punch"
    assert p.threshold_usd == pytest.approx(12e6)
    assert p.direction == "above"


def test_parse_mcap_watch_mint():
    t = "So11111111111111111111111111111111111111112 市值突破 5m 时提醒我"
    p = parse_mcap_watch_intent(t)
    assert p is not None
    assert "So111" in p.token_query
    assert p.threshold_usd == pytest.approx(5e6)


def test_parse_mcap_watch_below():
    t = "监控 BONK 市值跌破 500k 时通知我"
    p = parse_mcap_watch_intent(t)
    assert p is not None
    assert p.direction == "below"
    assert p.threshold_usd == pytest.approx(500_000)


def test_parse_list_and_cancel():
    assert parse_list_intent("/mcap_watches") is True
    assert parse_list_intent("/mcap_watches@MyBot") is True
    assert parse_cancel_intent("取消市值提醒 2") == 2
    assert parse_cancel_intent("/mcap_unwatch 1") == 1
    assert parse_cancel_intent("取消市值提醒2") == 2


def test_no_false_positive_short_chat():
    assert parse_mcap_watch_intent("你好，今天天气不错") is None
    assert parse_mcap_watch_intent("提醒我买牛奶") is None


def test_parsed_watch_from_dict_roundtrip():
    d = {
        "token_query": "x",
        "threshold_usd": 1e6,
        "direction": "below",
        "anchor_usd": None,
        "source_text": "t",
    }
    p = parsed_watch_from_dict(d)
    assert isinstance(p, ParsedWatchIntent)
    assert p.direction == "below"


def test_format_usd_compact():
    assert "M" in format_usd_compact(12e6) or "m" in format_usd_compact(12e6).lower()
