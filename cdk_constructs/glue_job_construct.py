""" Glue Construct. """

import os

from typing import (
    List,
    Dict
)

from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    core,
)

current_dir = os.path.dirname(__file__)

class GlueJobConstruct(core.Construct):
    """ Glue Job Construct. """
    @property
    def glue_handler(self):
        return self._glue_arn_dict

    def __init__(
        self,
        scope: core.Construct,
        id:str,
        artifact_bucket_name:str,
        glue_temp_bucket_name:str,
        script_key:str,
        allocated_capacity:int=5,
        max_concurrent_runs:int=5,
        s3_arns:List[str]=None,
        ddb_table_arns:List[str]=None,
        kms_key_arns:List[str]=None,
        schedule:str=None,
        arguments:Dict[str,str]=None,
        **kwargs) -> None:

        super().__init__(scope, id, **kwargs)
        self._glue_arn_dict = {}
        self.artifact_bucket_name = artifact_bucket_name
        self.glue_temp_bucket_name = glue_temp_bucket_name
        aws_region = os.environ.get("CDK_DEPLOY_REGION", os.environ["CDK_DEFAULT_REGION"])

        # Glue managed policy
        glue_managed_policy = iam.ManagedPolicy(
            self,
            id='glue-policy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetBucketLocation',
                        's3:ListBucket',
                        's3:ListAllMyBuckets',
                        's3:GetBucketAcl',
                        's3:GetObject',
                        's3:PutObject',
                        's3:DeleteObject',
                        'ec2:DescribeVpcEndpoints',
                        'ec2:DescribeRouteTables',
                        'ec2:CreateNetworkInterface',
                        'ec2:DeleteNetworkInterface',
                        'ec2:DescribeNetworkInterfaces',
                        'ec2:DescribeSecurityGroups',
                        'ec2:DescribeSubnets',
                        'ec2:DescribeVpcAttribute',
                        'iam:ListRolePolicies',
                        'iam:GetRole',
                        'iam:GetRolePolicy',
                        'logs:CreateLogGroup',
                        'logs:CreateLogStream',
                        'logs:PutLogEvents',
                        'logs:AssociateKmsKey',
                        'cloudwatch:PutMetricData'
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'glue:GetTable',
                        'glue:GetPartitions',
                        'glue:GetPartition',
                        'glue:GetTables',
                        'glue:UpdatePartition',
                        'glue:UpdateTable',
                        'glue:GetDatabase*',
                        'glue:GetJobRun*',
                        'glue:GetUserDefinedFunctions',
                        'glue:CreateDatabase',
                    ],
                    resources=[
                        f'arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:catalog',
                        f'arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:database/*',
                        f'arn:aws:glue:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:table/*'
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'glue:GetSecurityConfiguration'
                    ],
                    resources=["*"]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:CreateBucket"
                    ],
                    resources=[
                        "arn:aws:s3:::aws-glue-*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:PutObject",
                        "s3:DeleteObject"
                    ],
                    resources=[
                        "arn:aws:s3:::aws-glue-*/*",
                        "arn:aws:s3:::*/*aws-glue-*/*"
                    ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject"
                    ],
                    resources=[
                        "arn:aws:s3:::crawler-public*",
                        "arn:aws:s3:::aws-glue-*"
                    ]
                ),
                iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "s3:DeleteObject",
                            "s3:DeleteObjectTagging",
                            "s3:GetObject",
                            "s3:GetObjectTagging",
                            "s3:ListBucket",
                            "s3:ListBucketByTags",
                            "s3:PutObject",
                            "s3:PutObjectTagging",
                            "s3:PutObjectAcl"
                        ],
                        resources=[
                            f'arn:aws:s3:::{self.artifact_bucket_name}',
                            f'arn:aws:s3:::{self.artifact_bucket_name}/*',
                            f'arn:aws:s3:::{self.glue_temp_bucket_name}',
                            f'arn:aws:s3:::{self.glue_temp_bucket_name}/*'
                        ]
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["kms:Encrypt",
                            "kms:Decrypt",
                            "kms:ReEncrypt",
                            "kms:GenerateDataKey",
                            "kms:DescribeKey"
                        ],
                    resources=[f"arn:aws:kms:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:key/*"]
                )
            ]
        )

        glue_managed_policy = iam.ManagedPolicy.from_managed_policy_arn(self, "GlueCustomPolicy", glue_managed_policy.managed_policy_arn)

        self.glue_role = iam.Role(
            self, 
            f'{id}-role',
            role_name=f'data-gov-{aws_region}-{id}',
            description=f'Role for the {id}.',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                glue_managed_policy
            ]
        )

        if ddb_table_arns:
            self.glue_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'dynamodb:BatchGetItem',
                        'dynamodb:BatchWriteItem',
                        'dynamodb:DescribeTable',
                        'dynamodb:GetItem',
                        'dynamodb:Scan'
                    ],
                    resources=[ddb_table_arns]
                )
            )

        if kms_key_arns:
            self.glue_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey"
                    ],
                    resources=kms_key_arns
                )
            )

        if s3_arns:
            self.glue_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:DeleteObject",
                            "s3:DeleteObjectTagging",
                            "s3:GetObject",
                            "s3:GetObjectTagging",
                            "s3:ListBucket",
                            "s3:ListBucketByTags",
                            "s3:PutObject",
                            "s3:PutObjectTagging",
                            "s3:PutObjectAcl"
                            ],
                    resources=s3_arns
                )
            )

        self.glue_job = glue.CfnJob(
            self, id=f'{id}-glue-job',
            name=f'data-gov-{aws_region}-{id}',
            description=f'{id}',
            role=self.glue_role.role_name,
            allocated_capacity=allocated_capacity,
            execution_property=glue.CfnJob.ExecutionPropertyProperty(max_concurrent_runs=max_concurrent_runs),
            command=glue.CfnJob.JobCommandProperty(
                name='glueetl',
                python_version='3',
                script_location='s3://' + self.artifact_bucket_name + '/' + script_key
            ),
            glue_version="2.0",
            default_arguments=arguments
        )

        if schedule:
            glue_trigger=glue.CfnTrigger(
                self,
                id=f'{id}-trigger',
                name=f'data-gov-{id}-trigger',
                start_on_creation=True,
                type='SCHEDULED',
                schedule=schedule,
                actions=[glue.CfnTrigger.ActionProperty(job_name=self.glue_job.name)
                ]
            )

            self._glue_arn_dict[f'{id}_trigger_name'] = glue_trigger.name

        self._glue_arn_dict[f'{id}_glue_job_name'] = self.glue_job.name
        self._glue_arn_dict[f'{id}_glue_job_role_arn'] = self.glue_role.role_arn
