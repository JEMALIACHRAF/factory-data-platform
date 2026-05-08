"""
StorageStack – S3 buckets (raw, processed, scripts) + Glue Data Catalog
Production-grade: encryption, lifecycle, versioning, access logging
"""

from aws_cdk import (
    Stack, RemovalPolicy, Duration, Tags,
    aws_s3 as s3,
    aws_glue as glue,
    aws_kms as kms,
)
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, env_name: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── KMS key for S3 encryption ─────────────────────────────────────────
        self.kms_key = kms.Key(
            self, "DataKey",
            alias=f"factory-data-{env_name}",
            enable_key_rotation=True,
            description="Factory data platform encryption key",
        )

        # ── Access logs bucket (unencrypted per AWS requirement) ──────────────
        logs_bucket = s3.Bucket(
            self, "AccessLogs",
            bucket_name=f"factory-access-logs-{env_name}-{self.account}",
            removal_policy=RemovalPolicy.RETAIN,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            lifecycle_rules=[
                s3.LifecycleRule(expiration=Duration.days(90))
            ],
        )

        # ── Raw data bucket (Kafka S3 sink target) ────────────────────────────
        self.raw_bucket = s3.Bucket(
            self, "RawBucket",
            bucket_name=f"factory-raw-{env_name}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            server_access_logs_bucket=logs_bucket,
            server_access_logs_prefix="raw/",
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                # Hot: 0-30 days → Standard
                # Warm: 30-90 days → Standard-IA
                # Cold: 90-365 days → Glacier Instant Retrieval
                # Archive: 365+ days → Deep Archive
                s3.LifecycleRule(
                    id="tiered-storage",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(30),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER_INSTANT_RETRIEVAL,
                            transition_after=Duration.days(90),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.DEEP_ARCHIVE,
                            transition_after=Duration.days(365),
                        ),
                    ],
                    noncurrent_version_expiration=Duration.days(30),
                ),
            ],
            cors=[
                s3.CorsRule(
                    allowed_methods=[s3.HttpMethods.GET, s3.HttpMethods.PUT],
                    allowed_origins=["*"],
                    allowed_headers=["*"],
                    max_age=3600,
                )
            ],
        )

        # ── Processed bucket (Glue output, Parquet/Delta) ─────────────────────
        self.processed_bucket = s3.Bucket(
            self, "ProcessedBucket",
            bucket_name=f"factory-processed-{env_name}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            server_access_logs_bucket=logs_bucket,
            server_access_logs_prefix="processed/",
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="parquet-tiering",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                            transition_after=Duration.days(60),
                        ),
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER_INSTANT_RETRIEVAL,
                            transition_after=Duration.days(180),
                        ),
                    ],
                    noncurrent_version_expiration=Duration.days(7),
                ),
            ],
        )

        # ── Scripts bucket (Glue job scripts) ─────────────────────────────────
        self.scripts_bucket = s3.Bucket(
            self, "ScriptsBucket",
            bucket_name=f"factory-scripts-{env_name}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=self.kms_key,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=RemovalPolicy.DESTROY,  # Scripts are in git
        )

        # ── Glue Database ─────────────────────────────────────────────────────
        self.glue_database = glue.CfnDatabase(
            self, "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"factory_datalake_{env_name}",
                description="Factory IoT datalake – raw + processed layers",
            ),
        )

        # ── Tags ──────────────────────────────────────────────────────────────
        for bucket in [self.raw_bucket, self.processed_bucket, self.scripts_bucket]:
            Tags.of(bucket).add("Project", "FactoryDataPlatform")
            Tags.of(bucket).add("Environment", env_name)
            Tags.of(bucket).add("CostCenter", "DE-TEAM")
