"""Unit tests for pipeline.claude_stream_json NDJSON aggregation."""

from pipeline.claude_stream_json import (
    StreamJsonAccum,
    feed_stream_json_event,
    stream_accum_final_text,
)


def test_feed_assistant_and_result():
    acc = StreamJsonAccum()
    feed_stream_json_event(
        {
            "type": "assistant",
            "message": {
                "content": [{"type": "text", "text": "partial"}],
            },
            "session_id": "abc-123",
        },
        acc,
    )
    assert acc.last_assistant_text == "partial"
    assert acc.session_id == "abc-123"
    feed_stream_json_event(
        {
            "type": "result",
            "result": "final answer",
            "is_error": False,
            "session_id": "abc-123",
        },
        acc,
    )
    assert stream_accum_final_text(acc) == "final answer"


def test_rate_limit_sets_flag():
    acc = StreamJsonAccum()
    feed_stream_json_event({"type": "rate_limit_event", "session_id": "s"}, acc)
    assert acc.hit_rate_limit
    assert acc.is_error
