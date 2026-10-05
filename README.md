# LearnStream

An event-driven learning-analytics pipeline on AWS. Learning apps emit events
(a lesson started, a question answered); this service ingests them, counts them
per learner, and lands hourly aggregates in S3 for querying.

It is built around the two properties that make event pipelines hard in
practice: **duplicate delivery** and **partial failure**.

```
POST /events
     |
  API Gateway  ──▶  ingest Lambda  ──▶  SQS  ──▶  worker Lambda  ──▶  DynamoDB
                    (validate)          │ DLQ     (idempotent)          │
                                        │                               │
                    EventBridge (hourly) ──▶ rollup Lambda ──▶ S3 (Athena-ready)
```

## Delivery semantics

| Concern | How it is handled |
|---|---|
| Duplicate delivery | SQS standard queues are at-least-once. Each event is written with `ConditionExpression: attribute_not_exists(pk)`. A duplicate fails the condition, is acknowledged, and does **not** increment the aggregate. |
| Poison messages | A body that can never parse is logged and dropped rather than retried, so it cannot occupy the queue for three delivery attempts and then fill the DLQ. |
| Transient failures | A throttled or failed write returns `batchItemFailures`, so SQS redelivers **that one message** instead of the whole batch of ten. |
| Exhausted retries | After 3 receives, the message moves to the DLQ, which has a CloudWatch alarm at depth > 0. |
| Backpressure | The SQS event source caps the worker at `max_concurrency=2`, so a traffic spike cannot fan out unbounded Lambda concurrency onto the table, without reserving capacity from the account pool. Queue age is alarmed at 5 minutes. |
| Producer retries | If the producer supplies `event_id`, retries are idempotent. Without one, the server mints an id and a retry counts twice. Producers should send an id. |

## Layout

```
src/common/events.py   validation and the LearningEvent type (stdlib only)
src/ingest/handler.py  API Gateway -> SQS
src/worker/handler.py  SQS -> DynamoDB, idempotent, partial batch failure
src/rollup/handler.py  DynamoDB -> S3, hourly, Hive-partitioned
infra/                 AWS CDK stack (Python)
tests/                 pytest + moto, no AWS account needed
loadtest/              Locust scenario incl. duplicate and invalid traffic
```

## Run the tests

```bash
pip install -r requirements-dev.txt
pytest --cov=src --cov-report=term-missing
```

The tests use `moto`, so they run offline. They cover validation, the ingest
edge, idempotent replay, poison-message handling, transient-failure retry, and
the rollup writer.

## Deploy

```bash
npm install -g aws-cdk
cd infra
cdk bootstrap          # once per account/region
cdk deploy
```

Outputs include the API URL, the dashboard URL, and the queue/table/bucket
names. Tear down with `cdk destroy` when you are not measuring; everything
here is either free-tier or pay-per-request, but an idle stack still costs a
few cents a month in CloudWatch.

For CI deploys, create a GitHub OIDC role in IAM and set `AWS_DEPLOY_ROLE_ARN`
as a repository secret. The workflow in `.github/workflows/ci.yml` lints,
tests with a coverage floor, synthesizes the stack on every PR, and deploys
only from `main`.

## Results

Measured against the deployed stack: **38.4 events/second, p99 160 ms, 0 failures over 6,898 requests**, 25 replays of one event id storing exactly 1, 50/50 events recovered after a DLQ redrive, 98% test coverage. See [RESULTS.md](RESULTS.md), which also documents the four bugs that only appeared once it was deployed.

## Next increments

- Replace the rollup scan with a DynamoDB Streams consumer (the scan is fine at
  demo scale and becomes the bottleneck at real scale).
- Add a Glue table with partition projection so Athena queries need no crawler.
- Add X-Ray tracing and a service map.
- Add a canary (`POST /events` every minute) and alarm on its failure.
