#!/usr/bin/env python3
import os
from dotenv import load_dotenv
import aws_cdk as cdk
from stacks.network_stack import NetworkStack
from stacks.storage_stack import StorageStack
from stacks.glue_stack import GlueStack
from stacks.redshift_stack import RedshiftStack
from stacks.monitoring_stack import MonitoringStack

# Charge le .env depuis la racine du projet
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

app = cdk.App()

# Lit depuis .env ou depuis les context params CDK
account = (
    app.node.try_get_context("account")
    or os.getenv("AWS_ACCOUNT_ID")
    or os.getenv("CDK_DEFAULT_ACCOUNT")
)
region = (
    app.node.try_get_context("region")
    or os.getenv("AWS_REGION")
    or os.getenv("CDK_DEFAULT_REGION")
    or "eu-west-1"
)
env_name = (
    app.node.try_get_context("env")
    or os.getenv("ENV_NAME")
    or "prod"
)
alert_email = (
    app.node.try_get_context("alert_email")
    or os.getenv("ALERT_EMAIL")
    or "data-team@company.com"
)

# Enable Streaming stack only when explicitly requested
# MSK (Kafka) requires account subscription — skip in CI
enable_streaming = (
    app.node.try_get_context("enable_streaming") == "true"
    or os.getenv("ENABLE_STREAMING", "false").lower() == "true"
)

env = cdk.Environment(account=account, region=region)

network = NetworkStack(
    app, f"FactoryNetwork-{env_name}",
    env=env, env_name=env_name,
)

storage = StorageStack(
    app, f"FactoryStorage-{env_name}",
    env=env, env_name=env_name,
)
storage.add_dependency(network)

# Streaming stack (MSK Kafka) — disabled by default
# Enable with: --context enable_streaming=true
# or ENABLE_STREAMING=true in .env
if enable_streaming:
    from stacks.streaming_stack import StreamingStack
    streaming = StreamingStack(
        app, f"FactoryStreaming-{env_name}",
        vpc=network.vpc,
        env=env, env_name=env_name,
    )
    streaming.add_dependency(storage)

glue = GlueStack(
    app, f"FactoryGlue-{env_name}",
    raw_bucket=storage.raw_bucket,
    processed_bucket=storage.processed_bucket,
    scripts_bucket=storage.scripts_bucket,
    env=env, env_name=env_name,
)
glue.add_dependency(storage)

redshift = RedshiftStack(
    app, f"FactoryRedshift-{env_name}",
    vpc=network.vpc,
    processed_bucket=storage.processed_bucket,
    env=env, env_name=env_name,
)
redshift.add_dependency(glue)

monitoring = MonitoringStack(
    app, f"FactoryMonitoring-{env_name}",
    glue_jobs=glue.job_names,
    env=env, env_name=env_name,
)
monitoring.add_dependency(redshift)

app.synth()