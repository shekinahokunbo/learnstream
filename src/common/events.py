"""Shared event parsing and validation.

Deliberately dependency-free: Lambda runtimes ship boto3 only, so keeping this
module to the standard library means no build step and no layer to maintain.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from dataclasses import dataclass, asdict
from typing import Any

# Event types the pipeline accepts. Anything else is rejected at the edge so a
# bad producer cannot poison the queue.
VALID_EVENT_TYPES = frozenset(
    {"lesson_started", "lesson_completed", "question_answered", "session_ended"}
)

_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")

MAX_PAYLOAD_BYTES = 8 * 1024


class ValidationError(ValueError):
    """Raised when an inbound event fails validation."""


@dataclass(frozen=True)
class LearningEvent:
    event_id: str
    user_id: str
    event_type: str
    occurred_at: int  # epoch seconds, producer-supplied
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), separators=(",", ":"), sort_keys=True)


def parse_event(raw: Any, *, now: int | None = None) -> LearningEvent:
    """Validate a raw inbound event and normalise it into a LearningEvent.

    The producer may supply event_id for idempotent retries. When it does not,
    we mint one, which means an un-keyed retry is treated as a new event. That
    tradeoff is documented in README.md under "Delivery semantics".
    """
    if isinstance(raw, (str, bytes)):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValidationError(f"body is not valid JSON: {exc}") from exc

    if not isinstance(raw, dict):
        raise ValidationError("event must be a JSON object")

    user_id = raw.get("user_id")
    if not isinstance(user_id, str) or not _ID_RE.match(user_id):
        raise ValidationError("user_id must match [A-Za-z0-9_-]{1,64}")

    event_type = raw.get("event_type")
    if event_type not in VALID_EVENT_TYPES:
        raise ValidationError(
            f"event_type must be one of {sorted(VALID_EVENT_TYPES)}"
        )

    event_id = raw.get("event_id") or str(uuid.uuid4())
    if not isinstance(event_id, str) or not _ID_RE.match(event_id):
        raise ValidationError("event_id must match [A-Za-z0-9_-]{1,64}")

    occurred_at = raw.get("occurred_at", now if now is not None else int(time.time()))
    if not isinstance(occurred_at, int) or isinstance(occurred_at, bool):
        raise ValidationError("occurred_at must be an integer epoch timestamp")
    if occurred_at <= 0:
        raise ValidationError("occurred_at must be positive")

    payload = raw.get("payload", {})
    if not isinstance(payload, dict):
        raise ValidationError("payload must be an object")
    if len(json.dumps(payload)) > MAX_PAYLOAD_BYTES:
        raise ValidationError(f"payload exceeds {MAX_PAYLOAD_BYTES} bytes")

    return LearningEvent(
        event_id=event_id,
        user_id=user_id,
        event_type=event_type,
        occurred_at=occurred_at,
        payload=payload,
    )
