"""Centralized Resources App."""
import os
import json
from typing import List
from aws_cdk import (
    core,
    aws_s3,
    aws_s3_deployment,
    aws_iam,
    aws_kms,
    aws_s3_notifications,
    aws_sns
)
from cdk_constructs.lambda_construct import LambdaConstruct
from accounts.accounts import (
    fetch_account_catalogs,
    fetch_account_streamers
)
from definitions.definition import DefinitionSet

ACCOUNT_NUMBER = os.environ.get('CDK_DEPLOY_ACCOUNT')

class CentralizedResources(core.Stack):
    """Centralized Resources Stack."""

    def __init__(
            self,
            scope: core.Construct,
            id: str, # pylint: disable=redefined-builtin
            bucket_name: str,
            sns_topic_name: str,
            external_roles: List[str],
            metric_frequencies: list,
            **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)
        self.bucket_name = bucket_name
        self.sns_topic_name = sns_topic_name
        self.bucket_kms_key=aws_kms.Key(
            self,
            id='kms-key',
            removal_policy=core.RemovalPolicy.RETAIN,
            enabled=True,
            enable_key_rotation=True
        )
        self.lifecycle_rules=[]
        for frequency in metric_frequencies:
            if frequency == "minute":
                self.lifecycle_rules.append(aws_s3.LifecycleRule(
                    id="metrics_minute",
                    prefix="metrics/minute/*",
                    expiration=core.Duration.days(2)
                ))
            if frequency == "hour":
                self.lifecycle_rules.append(aws_s3.LifecycleRule(
                    id="metrics_hour",
                    prefix="metrics/hour/*",
                    expiration=core.Duration.days(30)
                ))
            if frequency == "day":
                self.lifecycle_rules.append(aws_s3.LifecycleRule(
                    id="metrics_day",
                    prefix="metrics/day/*",
                    expiration=core.Duration.days(90)
                ))

        self.storage = aws_s3.Bucket(
            self,
            id='CentralizedMonitoringBucket',
            bucket_name=self.bucket_name,
            encryption=aws_s3.BucketEncryption.KMS,
            encryption_key=self.bucket_kms_key,
            versioned=True,
            lifecycle_rules=self.lifecycle_rules
        )

        for account in fetch_account_streamers(ACCOUNT_NUMBER):
            access_roles = ['arn:aws:iam::*:role/date-gov-*'] + external_roles

            self.storage.encryption_key.add_to_resource_policy(
                statement=aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    sid='read_write',
                    actions=[
                        'kms:Encrypt',
                        'kms:Decrypt',
                        'kms:ReEncrypt',
                        'kms:GenerateDataKey*',
                        'kms:DescribeKey'
                    ],
                    resources=[
                        '*'
                    ],
                    principals=[aws_iam.ArnPrincipal(f"arn:aws:iam::{account}:root")],
                    conditions={
                        'ArnLike':{
                            'aws:PrincipalArn': access_roles
                        }
                    }
                )
            )

            self.storage.add_to_resource_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=["s3:*"], # TODO: Restrict
                    resources=[self.storage.bucket_arn,
                            self.storage.bucket_arn+'/*'],
                    principals=[aws_iam.ArnPrincipal(f"arn:aws:iam::{account}:root")],
                    conditions={
                        'ArnLike':{
                            'aws:PrincipalArn': access_roles
                        }
                    }
                )
            )

        lambda_id='add-partition'
        self.add_partition = LambdaConstruct(
            self, id=lambda_id,
            code='lambda/',
            handler='add_partition.main',
            timeout=10,
            memory_size=128,
            environment={
                "catalogs": ",".join(fetch_account_catalogs(ACCOUNT_NUMBER))
            }
        )

        self.storage.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED_PUT,
            aws_s3_notifications.LambdaDestination(self.add_partition.return_lambda_function),
            aws_s3.NotificationKeyFilter(prefix="metrics/")
        )

        self.storage.add_event_notification(
            aws_s3.EventType.OBJECT_CREATED_PUT,
            aws_s3_notifications.LambdaDestination(self.add_partition.return_lambda_function),
            aws_s3.NotificationKeyFilter(prefix="slas/")
        )

        self.deploy_definitions_metadata()

        self.sns_topic = aws_sns.Topic(
                self,
                'centralized-alarm-sns-topic',
                topic_name=self.sns_topic_name
            )

        for account in fetch_account_streamers(ACCOUNT_NUMBER):
            publish_roles = [f'arn:aws:iam::{account}:role/data-gov-*']

            self.sns_topic.add_to_resource_policy(
                statement=aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    sid=f'publish_{account}',
                    actions=[
                        'sns:Publish'
                    ],
                    resources=[
                        '*'
                    ],
                    principals=[aws_iam.ArnPrincipal(f"arn:aws:iam::{account}:root")],
                    conditions={
                        'ArnLike':{
                            'aws:PrincipalArn': publish_roles
                        }
                    }
                )
            )
    def deploy_definitions_metadata(self):
        """ Collects and deploys definitons metadata to AWS Central account. """

        definition_set = DefinitionSet(account=ACCOUNT_NUMBER)
        if not os.path.exists('cdk.out/definitions/metrics'):
            os.makedirs('cdk.out/definitions/metrics')
        if not os.path.exists('cdk.out/definitions/slas'):
            os.makedirs('cdk.out/definitions/slas')
        with open('cdk.out/definitions/metrics/metrics.json', 'w') as f:
            json.dump(definition_set.metric_sets, f)
        with open('cdk.out/definitions/slas/slas.json', 'w') as f:
            json.dump(definition_set.sla_sets, f)

        prefix = 'definitions/'
        aws_s3_deployment.BucketDeployment(
            self, 's3DeployDefenitions',
            sources=[
                aws_s3_deployment.Source.asset('cdk.out/definitions/'),
            ],
            destination_bucket=self.storage,
            destination_key_prefix=prefix
        )
