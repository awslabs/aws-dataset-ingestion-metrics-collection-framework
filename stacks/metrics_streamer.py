"""Metric Streamer App."""
import os
import zipfile
import textwrap
import urllib.request
from typing import List
from aws_cdk import (
    core,
    aws_events,
    aws_events_targets,
    aws_s3,
    aws_s3_deployment,
    aws_iam,
    aws_cloudwatch,
    aws_sns,
    aws_sns_subscriptions
)
from cdk_constructs.cw_alarm import Alarm
from cdk_constructs.cw_metric import CwMetric
from cdk_constructs.glue_job_construct import GlueJobConstruct
from cdk_constructs.kinesis_construct import KinesisConstruct
from cdk_constructs.metric_streamer_construct import MetricStreamerConstruct
from cdk_constructs.sla_streamer_construct import SLAStreamerConstruct
from cdk_constructs.sla_parse_construct import SlaParseConstruct
from cdk_constructs.glue_catalog_construct import GlueCatalogConstruct
from accounts.accounts import fetch_account_central

from dataquality.metric import BusinessMetric

from definitions.definition import Definition

ACCOUNT_NUMBER = os.environ.get('CDK_DEPLOY_ACCOUNT')
CENTRAL_ACCOUNT_NUMBER = fetch_account_central(ACCOUNT_NUMBER)

definition = Definition(account=ACCOUNT_NUMBER)

current_dir = os.path.dirname(__file__)

class MetricStreamer(core.Stack):
    """Metric Streamer Stack."""

    def __init__(
            self,
            scope: core.Construct,
            id: str, # pylint: disable=redefined-builtin
            bucket_name: str,
            sns_topic_name: str,
            central_account_dashboards: int,
            metric_frequencies: list,
            external_roles: List[str],
            **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # Access the current stage name.
        self.bucket_name = bucket_name
        self.sns_topic_name = sns_topic_name
        self.bucket_arn = f'arn:aws:s3:::{self.bucket_name}'
        self.cross_account = fetch_account_central(ACCOUNT_NUMBER)
        self.central_account_dashboards = central_account_dashboards
        self.metric_frequencies = metric_frequencies

        #Provisions Metric Streameing resources
        self.provision_metrics_streaming_resources()

        #Provisions SLA Streaming resources
        self.provision_slas_streaming_resources()

        #Provisions Metric Producer resources
        self.provision_business_metrics_producing_resources()

        #Generate CW Alarms
        self.generate_alarms()

        #Generate Dashboard and widgets
        self.generate_dashboard()

        #Generate Glue Catalog
        self.generate_glue_catalog()

    def provision_metrics_streaming_resources(self):
        """ Provisions metrics streaming AWS resources. """

        stream_dict = {}
        for frequency in self.metric_frequencies:
            kinesis_resources_frequency = KinesisConstruct(
                self,
                f'kinesis-streaming-resources-{frequency}',
                bucket_arn = self.bucket_arn,
                prefix = f'metrics/{frequency}/'+core.Aws.REGION+'/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/!{timestamp:HH}/',
                errorOutputPrefix = f'metric_errors/{frequency}/!{{timestamp:yyyy}}/!{{timestamp:MM}}/!{{timestamp:dd}}/!{{timestamp:HH}}/!{{firehose:error-output-type}}',
                database='data_governance',
                table=f'metrics_{frequency}'
            )

            stream_dict[f"KINESIS_{frequency.upper()}_STREAM_NAME"] = kinesis_resources_frequency.kinesis_stream.stream_name
            stream_dict[f"KINESIS_{frequency.upper()}_STREAM_ARN"] = kinesis_resources_frequency.kinesis_stream.stream_arn

        _lambda_resource = MetricStreamerConstruct(
            self,
            'metrics_publishing_lambda',
            stream_dict
        )

        aws_events.Rule(
            self,
            id='event_rule-day',
            schedule=aws_events.Schedule.expression('cron(0 0 * * ? *)'), # top of every day (midnight)
            targets=[aws_events_targets.LambdaFunction(
                handler=_lambda_resource.function,
                event=aws_events.RuleTargetInput.from_object({'frequency': 'day'})
            )]
        )

        aws_events.Rule(
            self,
            id='event_rule-hour',
            schedule=aws_events.Schedule.expression('cron(0 * * * ? *)'), # top of every hour
            targets=[aws_events_targets.LambdaFunction(
                handler=_lambda_resource.function,
                event=aws_events.RuleTargetInput.from_object({'frequency': 'hour'})
            )]
        )

        aws_events.Rule(
            self,
            id='event_rule-minute',
            schedule=aws_events.Schedule.expression('cron(0/1 * * * ? *)'), # top of every minute
            targets=[aws_events_targets.LambdaFunction(
                handler=_lambda_resource.function,
                event=aws_events.RuleTargetInput.from_object({'frequency': 'minute'})
            )]
        )

    def provision_slas_streaming_resources(self):
        """ Provisions metrics streaming AWS resources. """

        sla_kinesis_resources = KinesisConstruct(
            self,
            'sla-kinesis-streaming-resources',
            bucket_arn = self.bucket_arn,
            prefix = 'slas/'+core.Aws.REGION+'/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/!{timestamp:HH}/',
            errorOutputPrefix = 'sla_errors/!{timestamp:yyyy}/!{timestamp:MM}/!{timestamp:dd}/!{timestamp:HH}/!{firehose:error-output-type}',
            database='data_governance',
            table='slas'
        )

        _sla_lambda_resource = SLAStreamerConstruct(
            self,
            'slas_publishing_lambda',
            stream_name=sla_kinesis_resources.kinesis_stream.stream_name,
            stream_arn=sla_kinesis_resources.kinesis_stream.stream_arn
        )

        aws_events.Rule(
            self,
            id='sla_event_rule',
            schedule=aws_events.Schedule.cron(), # every minute
            targets=[aws_events_targets.LambdaFunction(
                handler=_sla_lambda_resource.function
            )]
        )

    def provision_business_metrics_producing_resources(self):
        """ Produces business metrics resources. """

        # Artifact bucket
        artifact_bucket = aws_s3.Bucket(
            self,
            'artifact',
            bucket_name=f'data-governance-atrifact-{core.Aws.REGION}-{ACCOUNT_NUMBER}',
            versioned=True,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Glue temp bucket
        glue_temp_bucket = aws_s3.Bucket(
            self,
            'glue-temp',
            bucket_name=f'data-governance-glue-{core.Aws.REGION}-{ACCOUNT_NUMBER}',
            versioned=True,
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        if not os.path.exists('cdk.out/glue'):
            os.makedirs('cdk.out/glue')

        with urllib.request.urlopen('https://s3.us-west-2.amazonaws.com/crawler-public/json/serde/json-serde.jar') as f:
            jsonserde = f.read()
        with open('cdk.out/glue/json-serde.jar','wb') as output:
            output.write(jsonserde)

        # Data Governance zip
        data_governance = zipfile.ZipFile('cdk.out/glue/dataquality.zip', 'w')
        for root, _dirs, files in os.walk("dataquality"):
            for filename in files:
                if '__pycache__' not in root:
                    path = os.path.join(root, filename)
                    data_governance.write(path, path)
        data_governance.close()

        # Definitions zip
        definitions = zipfile.ZipFile('cdk.out/glue/definitions.zip', 'w')
        for root, _dirs, files in os.walk("definitions"):
            for filename in files:
                if '__pycache__' not in root:
                    path = os.path.join(root, filename)
                    definitions.write(path, path)
        definitions.close()

        # accounts zip
        accounts = zipfile.ZipFile('cdk.out/glue/accounts.zip', 'w')
        for root, _dirs, files in os.walk("accounts"):
            for filename in files:
                if '__pycache__' not in root:
                    path = os.path.join(root, filename)
                    accounts.write(path, path)
        accounts.close()

        # Artifacts Deployment
        prefix = 'glue/'
        aws_s3_deployment.BucketDeployment(
            self, 's3DeployExample',
            sources=[
                aws_s3_deployment.Source.asset('glue/'),
                aws_s3_deployment.Source.asset('cdk.out/glue/')
            ],
            destination_bucket=artifact_bucket,
            destination_key_prefix=prefix
        )

        for metric_set in definition.metric_sets:
            for metric in metric_set.metrics:
                if isinstance(metric, BusinessMetric):
                    GlueJobConstruct(
                        self,
                        f'data-gov-{metric_set.name}',
                        artifact_bucket_name=artifact_bucket.bucket_name,
                        glue_temp_bucket_name=glue_temp_bucket.bucket_name,
                        script_key='glue/business_metrics.py',
                        max_concurrent_runs=1,
                        schedule=metric_set.schedule,
                        arguments={
                            "--extra-py-files": f's3://{artifact_bucket.bucket_name}/glue/definitions.zip,s3://{artifact_bucket.bucket_name}/glue/dataquality.zip,s3://{artifact_bucket.bucket_name}/glue/accounts.zip',
                            "--extra-jars": f's3://{artifact_bucket.bucket_name}/glue/json-serde.jar',
                            "--TempDir": f's3://{glue_temp_bucket.bucket_name}',
                            "--account_number": ACCOUNT_NUMBER,
                            "--metric_set_name": metric_set.name,
                            "--enable-glue-datacatalog": ""
                        }
                    )
                    break

    def generate_alarms(self):
        """ Generate Alarms for every metric. """

        self.sns_topic = aws_sns.Topic(
                self,
                'sns-topic'
            )

        _parse_sla = SlaParseConstruct(
            self,
            'parse_sla_lambda',
            central_account_number=CENTRAL_ACCOUNT_NUMBER,
            central_sns_topic=self.sns_topic_name
        )

        self.sns_topic.add_subscription(
            aws_sns_subscriptions.LambdaSubscription(
                _parse_sla.function
            )
        )

        alarms_list = []
        for sla_set in definition.sla_sets:
            for sla in sla_set.slas:

                alarms_list.append(Alarm(
                    self,
                    id=sla.metric.alarm_unique_id(),
                    sla=sla,
                    sns_topic=self.sns_topic
                ))

        return alarms_list

    def generate_dashboard_sets(self):
        """ Generates dashboard_name and dashboard_category list. """
        dashboards_category_list = []
        dashboard_name_list = []
        for metric_set in definition.metric_sets:
            for metric in metric_set.metrics:
                dashboard_name_list.append(
                    metric.dashboard.dashboard_name
                )
                dashboard_category = getattr(metric.dashboard, 'dashboard_category')
                if dashboard_category:
                    dashboards_category_list.append(
                        metric.dashboard.dashboard_category
                    )

        return dashboard_name_list, dashboards_category_list

    def generate_dashboard(self):
        """ Generate CW dashboards"""

        _cw_role = aws_iam.Role(
            self,
            'role',
            assumed_by=aws_iam.AccountPrincipal(self.central_account_dashboards),
            role_name='CloudWatch-CrossAccountSharingRole'
        )

        _cfn_role = _cw_role.node.default_child

        #Deploy the CW cross acct role only via us-east-1 stack
        #SRINI TO REVIEW
        _cfn_role.cfn_options.condition = core.CfnCondition(
            self,
            "regioncheck",
            expression=core.Fn.condition_equals(core.Aws.REGION, "us-east-1")
        )

        managed_policies = [
            'CloudWatchReadOnlyAccess',
            'CloudWatchAutomaticDashboardsAccess'
        ]

        for policy in managed_policies:
            _cw_role.add_managed_policy(
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    managed_policy_name=policy
                )
            )

        dashboard_name_list, dashboards_category_list = self.generate_dashboard_sets()

        for dashboard in set(dashboard_name_list):
            widges_list = []
            for metric_set in definition.metric_sets:
                for metric in metric_set.metrics:
                    dashboard_name = metric.dashboard.dashboard_name
                    if dashboard_name == dashboard:
                        widges_list.append(
                            aws_cloudwatch.GraphWidget(
                                left=[CwMetric(
                                    self,
                                    id=f'data-gov-{metric.unique_id()}',
                                    metric = metric
                                    ).cw_metric
                                ],
                                title=metric.widget_title(),
                                width=10
                            )
                        )
                    else:
                        continue

            self.dashboard = aws_cloudwatch.Dashboard(
                self,
                'dataset-dbs-'+dashboard,
                dashboard_name=f'{dashboard}-{core.Aws.REGION}',
            )
            for widget in widges_list:
                self.dashboard.add_widgets(
                    widget
                )

        for dashboard_cat in set(dashboards_category_list):
            widges_list = []
            temp_list = []
            for metric_set in definition.metric_sets:
                for metric in metric_set.metrics:
                    dashboard_name = metric.dashboard.dashboard_name
                    dashboards_category = getattr(metric.dashboard, 'dashboard_category')
                    if dashboards_category:
                        if dashboard_cat == dashboards_category:
                            if dashboard_name not in temp_list:
                                markdown = """
                                ## Navigate to **{}** dashboard:   

                                *Click here for [button:primary:{}](https://{}.console.aws.amazon.com/cloudwatch/home?region={}#dashboards:name={};accountId={}) dashboard*
                                """.format(
                                    dashboard_name+'-'+core.Aws.REGION,
                                    dashboard_name+'-'+core.Aws.REGION,
                                    core.Aws.REGION,
                                    core.Aws.REGION,
                                    dashboard_name+'-'+core.Aws.REGION,
                                    core.Aws.ACCOUNT_ID
                                )
                                widges_list.append(
                                    aws_cloudwatch.TextWidget(
                                        markdown=textwrap.dedent(markdown),
                                        width = 12,
                                        height = 3
                                    )
                                )
                                markdown = ''
                                temp_list.append(dashboard_name)
                        else:
                            continue
                    else:
                        continue
            self.dashboard_cat = aws_cloudwatch.Dashboard(
                self,
                'category-dbs-'+dashboard_cat,
                dashboard_name=f'{dashboard_cat}-{core.Aws.REGION}',
            )
            for widget in widges_list:
                self.dashboard_cat.add_widgets(
                    widget
                )

    def generate_glue_catalog(self):
        """ Generate Glue catalog"""

        self.glue_catalog = GlueCatalogConstruct(
            self,
            id='DataGovCatalog',
            bucket_name=self.bucket_name,
            cross_account=self.cross_account,
            metric_frequencies=self.metric_frequencies
        )
