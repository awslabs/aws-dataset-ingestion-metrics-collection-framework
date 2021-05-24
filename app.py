"""Data Governance."""
import os
from aws_cdk import core

from stacks.metrics_streamer import MetricStreamer
from stacks.centralized_resources import CentralizedResources
from accounts.accounts import fetch_account_central

STACK=os.environ.get("STACK_NAME")
ACCOUNT=os.environ.get("CDK_DEPLOY_ACCOUNT", os.environ["CDK_DEFAULT_ACCOUNT"])
CENTRAL_ACCOUNT = fetch_account_central(ACCOUNT)
METRIC_FREQUENCIES = ["minute", "hour", "day"]

central_bucket=f'data-governance-{core.Aws.REGION}-{CENTRAL_ACCOUNT}'
central_sns_topic_name=f'data-governance-alarm-sns-{core.Aws.REGION}-{CENTRAL_ACCOUNT}'

ENV = core.Environment(
    account=ACCOUNT,
    region=os.environ.get("CDK_DEPLOY_REGION", os.environ["CDK_DEFAULT_REGION"])
)

app = core.App()

CentralizedResources(
    app,
    id="data-gov-central-resources",
    bucket_name=central_bucket,
    sns_topic_name=central_sns_topic_name,
    env=ENV,
    external_roles=[],
    metric_frequencies=METRIC_FREQUENCIES
)

MetricStreamer(
    app,
    id="data-gov-metrics-streamer",
    bucket_name=central_bucket,
    sns_topic_name=central_sns_topic_name,
    central_account_dashboards=CENTRAL_ACCOUNT,
    env=ENV,
    external_roles=[],
    metric_frequencies=METRIC_FREQUENCIES
)

app.synth()
