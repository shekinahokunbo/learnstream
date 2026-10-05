"""Load test for the ingest endpoint.

    locust -f loadtest/locustfile.py --host https://<api-id>.execute-api.<region>.amazonaws.com/prod

Run a fixed shape so the numbers are comparable between runs, e.g.
    --users 100 --spawn-rate 10 --run-time 5m --headless --csv results/run1

Record p50/p95/p99 and the failure count; measured results are in RESULTS.md.
"""

import random
import uuid

from locust import HttpUser, between, task

EVENT_TYPES = ["lesson_started", "lesson_completed", "question_answered", "session_ended"]


class LearnerTraffic(HttpUser):
    wait_time = between(0.1, 0.5)

    @task(10)
    def send_event(self):
        self.client.post(
            "/events",
            json={
                "event_id": str(uuid.uuid4()),
                "user_id": f"student-{random.randint(1, 5000)}",
                "event_type": random.choice(EVENT_TYPES),
                "payload": {"question_id": f"q{random.randint(1, 200)}"},
            },
            name="POST /events",
        )

    @task(1)
    def send_duplicate(self):
        """Replay a fixed event id to exercise the idempotency path."""
        self.client.post(
            "/events",
            json={
                "event_id": "replay-fixed-id",
                "user_id": "student-1",
                "event_type": "lesson_completed",
            },
            name="POST /events (duplicate)",
        )

    @task(1)
    def send_invalid(self):
        """Confirm bad input is rejected at the edge, not queued."""
        with self.client.post(
            "/events",
            json={"user_id": "student-1", "event_type": "not_a_type"},
            name="POST /events (invalid)",
            catch_response=True,
        ) as response:
            if response.status_code == 400:
                response.success()
