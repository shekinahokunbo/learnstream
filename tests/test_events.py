import json

import pytest

from common.events import MAX_PAYLOAD_BYTES, ValidationError, parse_event


def _valid(**overrides):
    base = {
        "user_id": "student-42",
        "event_type": "question_answered",
        "occurred_at": 1789000000,
        "payload": {"question_id": "q7", "correct": True},
    }
    base.update(overrides)
    return base


def test_parses_a_valid_event():
    event = parse_event(_valid(event_id="evt-1"))
    assert event.event_id == "evt-1"
    assert event.user_id == "student-42"
    assert event.payload["correct"] is True


def test_accepts_a_json_string_body():
    event = parse_event(json.dumps(_valid()))
    assert event.event_type == "question_answered"


def test_mints_an_event_id_when_absent():
    assert parse_event(_valid()).event_id


def test_defaults_occurred_at_to_now():
    payload = _valid()
    del payload["occurred_at"]
    assert parse_event(payload, now=123).occurred_at == 123


@pytest.mark.parametrize(
    "override, message",
    [
        ({"user_id": "has spaces"}, "user_id"),
        ({"user_id": 7}, "user_id"),
        ({"event_type": "not_a_real_type"}, "event_type"),
        ({"occurred_at": "yesterday"}, "occurred_at"),
        ({"occurred_at": -1}, "occurred_at"),
        ({"payload": "not-an-object"}, "payload"),
    ],
)
def test_rejects_invalid_events(override, message):
    with pytest.raises(ValidationError, match=message):
        parse_event(_valid(**override))


def test_rejects_a_boolean_timestamp():
    # bool is a subclass of int, so this needs its own guard.
    with pytest.raises(ValidationError):
        parse_event(_valid(occurred_at=True))


def test_rejects_an_oversized_payload():
    with pytest.raises(ValidationError, match="payload exceeds"):
        parse_event(_valid(payload={"blob": "x" * (MAX_PAYLOAD_BYTES + 1)}))


def test_rejects_malformed_json():
    with pytest.raises(ValidationError, match="not valid JSON"):
        parse_event("{nope")
