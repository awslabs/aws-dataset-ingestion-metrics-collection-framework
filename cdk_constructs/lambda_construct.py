""" Lambda Construct. """
import os
from typing import List
from aws_cdk import (
    aws_lambda,
    aws_iam,
    aws_sqs,
    aws_ec2,
    aws_lambda_event_sources,
    core
)

current_dir = os.path.dirname(__file__)

class LambdaConstruct(core.Construct):

    @property
    def lambdas_handler(self):
        return self._lambdas_arn_dict

    @property
    def return_lambda_function(self):
        return self._lambda_function

    def __init__(
        self,
        scope: core.Construct,
        id: str,
        code: str,
        handler: str,
        timeout: int,
        memory_size: int,
        vpc_id:str=None,
        security_group_ids:List[str]=None,
        managed_policy_arns:List[str]=None,
        cross_account_s3_arns:List[str]=None,
        event_sqs_queue_arn:str=None,
        kms_key_arns:List[str]=None,
        ddb_table_arns:List[str]=None,
        environment=None,
        **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        aws_region = os.environ.get("CDK_DEPLOY_REGION", os.environ["CDK_DEFAULT_REGION"])

        inline_policies = {
            'lambda-log-access-custom-policy': aws_iam.PolicyDocument(
                statements=[
                    aws_iam.PolicyStatement(
                        effect=aws_iam.Effect.ALLOW,
                        actions=[
                            'logs:CreateLogGroup', 
                            'logs:CreateLogStream', 
                            'logs:PutLogEvents'
                        ],
                        resources=[f'arn:aws:logs:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:*']
                    )
                ]
            ),
            'lambda-vpc-placement-policy': aws_iam.PolicyDocument(
                statements=[
                    aws_iam.PolicyStatement(
                        effect=aws_iam.Effect.ALLOW,
                        actions=[
                            'ec2:DescribeInstances',
                            'ec2:CreateNetworkInterface',
                            'ec2:AttachNetworkInterface',
                            'ec2:DescribeNetworkInterfaces',
                            'ec2:DeleteNetworkInterface'],
                        resources=['*']
                    ),
                    aws_iam.PolicyStatement(
                        effect=aws_iam.Effect.ALLOW,
                        actions=['autoscaling:CompleteLifecycleAction'],
                        resources=[f'arn:aws:autoscaling:{core.Aws.REGION}:{core.Aws.ACCOUNT_ID}:*']
                    )
                ]
            ),
            'glue-permission': aws_iam.PolicyDocument(
                statements=[
                    aws_iam.PolicyStatement(
                        effect=aws_iam.Effect.ALLOW,
                        actions=[
                            'glue:Get*',
                            'glue:Put*',
                            'glue:CreatePartition'
                        ],
                        resources=[
                            f'arn:aws:glue:{core.Aws.REGION}:*:catalog/*',
                            f'arn:aws:glue:{core.Aws.REGION}:*:crawler/*',
                            f'arn:aws:glue:{core.Aws.REGION}:*:catalog',
                            f'arn:aws:glue:{core.Aws.REGION}:*:database/*',
                            f'arn:aws:glue:{core.Aws.REGION}:*:table/*'
                        ]
                    )
                ]
            )
        }
 

        if managed_policy_arns:
            managed_policies=[aws_iam.ManagedPolicy.from_managed_policy_arn(self, arn, arn) for arn in managed_policy_arns]
        else:
            managed_policies=None

        self._lambda_role = aws_iam.Role(
            self,
            f'{id}_lambda_role',
            role_name=f'data-gov-{aws_region}-{id}',
            description=f'Lambda Role for {id}',
            assumed_by=aws_iam.ServicePrincipal('lambda.amazonaws.com'),
            inline_policies=inline_policies,
            managed_policies=managed_policies
        )

        if kms_key_arns:
            self._lambda_role.add_to_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
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

        if cross_account_s3_arns:
            self._lambda_role.add_to_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
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
                    resources=cross_account_s3_arns
                )
            )

        if ddb_table_arns:
            self._lambda_role.add_to_policy(
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=["dynamodb:DescribeTable", 
                             "dynamodb:PutItem",
                             "dynamodb:GetItem",
                             "dynamodb:UpdateItem"
                            ],
                    resources=ddb_table_arns
                )
            )

        if vpc_id:
            vpc = aws_ec2.Vpc.from_lookup(self, "rz_vpc", vpc_id=vpc_id)
        else:
            vpc=None

        if security_group_ids:
            security_groups=[aws_ec2.SecurityGroup.from_security_group_id(self, sg_id, sg_id) for sg_id in security_group_ids]
        else:
            security_groups=None

        self._lambda_function = aws_lambda.Function(
            self, id=id,
            description=id,
            runtime=aws_lambda.Runtime.PYTHON_3_8,
            code=aws_lambda.Code.asset(code),
            handler=f'{handler}',
            role=self._lambda_role,
            timeout=core.Duration.minutes(timeout),
            memory_size=memory_size,
            vpc=vpc,
            security_groups=security_groups,
            environment=environment
        )

        if event_sqs_queue_arn:
            queue = aws_sqs.Queue.from_queue_arn(self, f'{id}_sqs_queue', event_sqs_queue_arn)
            self._lambda_function.add_event_source(aws_lambda_event_sources.SqsEventSource(queue=queue, batch_size=1))

        self._lambdas_arn_dict = {
            f'{id}_lambda_arn': self._lambda_function.function_arn,
            f'{id}_lambda_role_arn': self._lambda_role.role_arn
        }
