"""LearnStream infrastructure.

  POST /events -> ingest Lambda -> SQS (+ DLQ) -> worker Lambda -> DynamoDB
                                                                     |
                                    EventBridge (hourly) -> rollup -> S3

Every Lambda is least-privilege: the ingest function can only send to the
queue, the worker can only write the table, the rollup can only read the table
and write the bucket.
"""

from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigw,
    aws_cloudwatch as cw,
    aws_dynamodb as ddb,
    aws_events as events,
    aws_events_targets as targets,
    aws_lambda as lambda_,
    aws_lambda_event_sources as sources,
    aws_s3 as s3,
    aws_sqs as sqs,
)
from constructs import Construct

import os

# Lambda source lives at the repo root, while cdk.json (and therefore the CDK
# working directory) lives in infra/, so resolve the asset path explicitly.
SRC_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")

WORKER_TIMEOUT = Duration.seconds(30)

# SQS event-source concurrency cap. Minimum accepted by Lambda is 2.
WORKER_MAX_CONCURRENCY = 2


class LearnStreamStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---------- storage ----------
        table = ddb.Table(
            self,
            "EventsTable",
            partition_key=ddb.Attribute(name="pk", type=ddb.AttributeType.STRING),
            sort_key=ddb.Attribute(name="sk", type=ddb.AttributeType.STRING),
            billing_mode=ddb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            point_in_time_recovery_specification=ddb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.DESTROY,  # demo project; keep data cheap
        )

        bucket = s3.Bucket(
            self,
            "AnalyticsBucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            encryption=s3.BucketEncryption.S3_MANAGED,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-raw-rollups", expiration=Duration.days(180)
                )
            ],
        )

        # ---------- queueing ----------
        dlq = sqs.Queue(
            self,
            "EventsDLQ",
            retention_period=Duration.days(14),
            enforce_ssl=True,
        )

        queue = sqs.Queue(
            self,
            "EventsQueue",
            # Must exceed the worker timeout, or SQS redelivers a message that
            # is still being processed.
            visibility_timeout=Duration.seconds(WORKER_TIMEOUT.to_seconds() * 6),
            retention_period=Duration.days(4),
            enforce_ssl=True,
            dead_letter_queue=sqs.DeadLetterQueue(max_receive_count=3, queue=dlq),
        )

        # ---------- compute ----------
        common_env = {"LOG_LEVEL": "INFO", "POWERTOOLS_SERVICE_NAME": "learnstream"}

        ingest_fn = lambda_.Function(
            self,
            "IngestFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(SRC_PATH),
            handler="ingest.handler.handler",
            timeout=Duration.seconds(10),
            memory_size=256,
            environment={**common_env, "QUEUE_URL": queue.queue_url},
            tracing=lambda_.Tracing.ACTIVE,
        )
        queue.grant_send_messages(ingest_fn)

        worker_fn = lambda_.Function(
            self,
            "WorkerFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(SRC_PATH),
            handler="worker.handler.handler",
            timeout=WORKER_TIMEOUT,
            memory_size=512,
            environment={**common_env, "TABLE_NAME": table.table_name},
            tracing=lambda_.Tracing.ACTIVE,
        )
        table.grant_write_data(worker_fn)
        worker_fn.add_event_source(
            sources.SqsEventSource(
                queue,
                batch_size=10,
                max_batching_window=Duration.seconds(5),
                report_batch_item_failures=True,
                # Backpressure: cap how many workers the queue may run at once,
                # so a traffic spike cannot fan out onto the table. Set here
                # rather than as reserved concurrency, because reserving takes
                # capacity out of the account-wide pool (10 by default) and
                # would starve the ingest function.
                max_concurrency=WORKER_MAX_CONCURRENCY,
            )
        )

        rollup_fn = lambda_.Function(
            self,
            "RollupFunction",
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.Code.from_asset(SRC_PATH),
            handler="rollup.handler.handler",
            timeout=Duration.minutes(5),
            memory_size=512,
            environment={
                **common_env,
                "TABLE_NAME": table.table_name,
                "BUCKET_NAME": bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )
        table.grant_read_data(rollup_fn)
        bucket.grant_put(rollup_fn)

        events.Rule(
            self,
            "RollupSchedule",
            schedule=events.Schedule.rate(Duration.hours(1)),
            targets=[targets.LambdaFunction(rollup_fn)],
        )

        # ---------- edge ----------
        api = apigw.RestApi(
            self,
            "LearnStreamApi",
            rest_api_name="learnstream",
            deploy_options=apigw.StageOptions(
                stage_name="prod",
                throttling_rate_limit=200,
                throttling_burst_limit=400,
                metrics_enabled=True,
            ),
        )
        api.root.add_resource("events").add_method(
            "POST", apigw.LambdaIntegration(ingest_fn)
        )

        # ---------- operational excellence ----------
        dlq_alarm = cw.Alarm(
            self,
            "DlqNotEmpty",
            metric=dlq.metric_approximate_number_of_messages_visible(
                period=Duration.minutes(5), statistic="Maximum"
            ),
            threshold=0,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="An event exhausted its retries and landed in the DLQ.",
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        worker_errors_alarm = cw.Alarm(
            self,
            "WorkerErrors",
            metric=worker_fn.metric_errors(period=Duration.minutes(5)),
            threshold=5,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        age_alarm = cw.Alarm(
            self,
            "QueueBacklogAge",
            metric=queue.metric_approximate_age_of_oldest_message(
                period=Duration.minutes(1), statistic="Maximum"
            ),
            threshold=300,  # seconds
            evaluation_periods=2,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
            alarm_description="Consumers are falling behind producers.",
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        )

        dashboard = cw.Dashboard(self, "LearnStreamDashboard", dashboard_name="learnstream")
        dashboard.add_widgets(
            cw.GraphWidget(
                title="API latency (p50 / p99)",
                left=[
                    api.metric_latency(statistic="p50"),
                    api.metric_latency(statistic="p99"),
                ],
            ),
            cw.GraphWidget(
                title="Queue depth and oldest message",
                left=[queue.metric_approximate_number_of_messages_visible()],
                right=[queue.metric_approximate_age_of_oldest_message()],
            ),
            cw.GraphWidget(
                title="Worker invocations and errors",
                left=[worker_fn.metric_invocations(), worker_fn.metric_errors()],
            ),
            cw.SingleValueWidget(
                title="DLQ depth",
                metrics=[dlq.metric_approximate_number_of_messages_visible()],
            ),
        )

        CfnOutput(self, "ApiUrl", value=f"{api.url}events")
        CfnOutput(self, "QueueUrl", value=queue.queue_url)
        CfnOutput(self, "DlqUrl", value=dlq.queue_url)
        CfnOutput(self, "TableName", value=table.table_name)
        CfnOutput(self, "BucketName", value=bucket.bucket_name)
        CfnOutput(
            self,
            "DashboardUrl",
            value=(
                f"https://{Aws.REGION}.console.aws.amazon.com/cloudwatch/home"
                f"?region={Aws.REGION}#dashboards:name=learnstream"
            ),
        )
        for alarm in (dlq_alarm, worker_errors_alarm, age_alarm):
            CfnOutput(self, f"{alarm.node.id}Arn", value=alarm.alarm_arn)
