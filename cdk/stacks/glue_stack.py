"""
GlueStack – AWS Glue jobs, crawlers, triggers, IAM role
Production setup: DPU auto-scaling, job bookmarks, retry logic, metrics
"""
from aws_cdk import (
    Stack, Duration, RemovalPolicy,
    aws_glue as glue,
    aws_glue_alpha as glue_alpha,
    aws_iam as iam,
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    aws_logs as logs,
)
from constructs import Construct
import os


class GlueStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        raw_bucket: s3.Bucket,
        processed_bucket: s3.Bucket,
        scripts_bucket: s3.Bucket,
        env_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.job_names: list[str] = []

        # ── IAM Role for Glue ─────────────────────────────────────────────────
        self.glue_role = iam.Role(
            self, "GlueRole",
            role_name=f"factory-glue-role-{env_name}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
            ],
        )

        raw_bucket.grant_read(self.glue_role)
        processed_bucket.grant_read_write(self.glue_role)
        scripts_bucket.grant_read(self.glue_role)

        # CloudWatch logs permission
        self.glue_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                resources=["arn:aws:logs:*:*:/aws-glue/*"],
            )
        )

        # ── Upload Glue scripts to S3 ─────────────────────────────────────────
        s3deploy.BucketDeployment(
            self, "UploadGlueScripts",
            sources=[s3deploy.Source.asset("../glue_jobs")],
            destination_bucket=scripts_bucket,
            destination_key_prefix="glue_jobs/",
        )

        # ── Common Glue job properties ────────────────────────────────────────
        common_args = {
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--spark-event-logs-path": f"s3://{scripts_bucket.bucket_name}/spark-logs/",
            "--enable-job-insights": "true",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-continuous-log-filter": "true",
            "--job-bookmark-option": "job-bookmark-enable",  # idempotency
            "--TempDir": f"s3://{scripts_bucket.bucket_name}/tmp/",
            "--conf": (
                "spark.sql.adaptive.enabled=true "
                "--conf spark.sql.adaptive.coalescePartitions.enabled=true "
                "--conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
            ),
        }

        # ── Job 1: Factory Log Processor ──────────────────────────────────────
        log_processor_job = glue.CfnJob(
            self, "LogProcessorJob",
            name=f"factory-log-processor-{env_name}",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{scripts_bucket.bucket_name}"
                    "/glue_jobs/log_processor.py"
                ),
            ),
            glue_version="4.0",
            worker_type="G.1X",          # 4 vCPU, 16 GB RAM per worker
            number_of_workers=5,          # scale up via trigger if needed
            max_retries=2,
            timeout=60,                   # minutes
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=3,
            ),
            default_arguments={
                **common_args,
                "--RAW_BUCKET": raw_bucket.bucket_name,
                "--PROCESSED_BUCKET": processed_bucket.bucket_name,
                "--DATABASE_NAME": f"factory_datalake_{env_name}",
                "--SOURCE_PREFIX": "logs/factory/",
                "--TARGET_PREFIX": "processed/factory_logs/",
                "--PARTITION_FORMAT": "year=%Y/month=%m/day=%d/hour=%H",
            },
        )
        self.job_names.append(log_processor_job.name)

        # ── Job 2: IoT Stream Transformer (Kafka→S3 dedup + enrich) ──────────
        iot_transformer_job = glue.CfnJob(
            self, "IotTransformerJob",
            name=f"factory-iot-transformer-{env_name}",
            role=self.glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=(
                    f"s3://{scripts_bucket.bucket_name}"
                    "/glue_jobs/iot_transformer.py"
                ),
            ),
            glue_version="4.0",
            worker_type="G.2X",          # heavier: 8 vCPU, 32 GB for joins
            number_of_workers=10,
            max_retries=1,
            timeout=120,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1,   # streaming job – 1 at a time
            ),
            default_arguments={
                **common_args,
                "--RAW_BUCKET": raw_bucket.bucket_name,
                "--PROCESSED_BUCKET": processed_bucket.bucket_name,
                "--DATABASE_NAME": f"factory_datalake_{env_name}",
                "--KAFKA_TOPIC": "factory-iot-events",
                "--WATERMARK_MINUTES": "5",
                "--ENABLE_DQ_CHECKS": "true",
            },
        )
        self.job_names.append(iot_transformer_job.name)

        # ── Glue Crawler – processed zone ────────────────────────────────────
        glue.CfnCrawler(
            self, "ProcessedCrawler",
            name=f"factory-processed-crawler-{env_name}",
            role=self.glue_role.role_arn,
            database_name=f"factory_datalake_{env_name}",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{processed_bucket.bucket_name}/processed/",
                        exclusions=["_temporary/**", "_spark*/**"],
                    )
                ]
            ),
            schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                update_behavior="UPDATE_IN_DATABASE",
                delete_behavior="LOG",
            ),
            configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}',
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 6 * * ? *)"  # daily 06:00 UTC
            ),
        )

        # ── Scheduled triggers ────────────────────────────────────────────────
        # Log processor: every hour
        glue.CfnTrigger(
            self, "LogProcessorTrigger",
            name=f"trigger-log-processor-{env_name}",
            type="SCHEDULED",
            schedule="cron(0 * * * ? *)",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=log_processor_job.name,
                    arguments={"--BATCH_SIZE": "10000"},
                )
            ],
            start_on_creation=True,
        )

        # IoT transformer: every 15 min
        glue.CfnTrigger(
            self, "IotTransformerTrigger",
            name=f"trigger-iot-transformer-{env_name}",
            type="SCHEDULED",
            schedule="cron(0/15 * * * ? *)",
            actions=[
                glue.CfnTrigger.ActionProperty(
                    job_name=iot_transformer_job.name,
                )
            ],
            start_on_creation=True,
        )
