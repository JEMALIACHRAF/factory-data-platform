#!/usr/bin/env python3
"""
Factory Data Platform - AWS CDK App
Production IoT pipeline: Kafka → S3 → Redshift + Glue processing
"""
import aws_cdk as cdk
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack
from stacks.streaming_stack import StreamingStack
from stacks.glue_stack import GlueStack
from stacks.redshift_stack import RedshiftStack
from stacks.monitoring_stack import MonitoringStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "eu-west-1",
)

env_name = app.node.try_get_context("env") or "prod"

# Layer 1 – Network
network = NetworkStack(app, f"FactoryNetwork-{env_name}", env=env, env_name=env_name)

# Layer 2 – Storage (S3 buckets, Glue catalog)
storage = StorageStack(app, f"FactoryStorage-{env_name}", env=env, env_name=env_name)
storage.add_dependency(network)

# Layer 3 – Streaming (MSK Kafka cluster)
streaming = StreamingStack(
    app, f"FactoryStreaming-{env_name}",
    vpc=network.vpc,
    env=env, env_name=env_name,
)
streaming.add_dependency(storage)

# Layer 4 – Glue ETL jobs
glue = GlueStack(
    app, f"FactoryGlue-{env_name}",
    raw_bucket=storage.raw_bucket,
    processed_bucket=storage.processed_bucket,
    scripts_bucket=storage.scripts_bucket,
    env=env, env_name=env_name,
)
glue.add_dependency(storage)

# Layer 5 – Redshift warehouse
redshift = RedshiftStack(
    app, f"FactoryRedshift-{env_name}",
    vpc=network.vpc,
    processed_bucket=storage.processed_bucket,
    env=env, env_name=env_name,
)
redshift.add_dependency(glue)

# Layer 6 – Monitoring
monitoring = MonitoringStack(
    app, f"FactoryMonitoring-{env_name}",
    glue_jobs=glue.job_names,
    env=env, env_name=env_name,
)
monitoring.add_dependency(redshift)

app.synth()
