""" Lambda Construct """
from aws_cdk import (
    core,
    aws_iam,
    aws_lambda
)

class SLAStreamerConstruct(core.Construct):
    """ Lambda Construct """
    stream_name: str

    def __init__(
        self,
        scope: core.Construct,
        id: str, # pylint: disable=redefined-builtin
        stream_name: str,
        stream_arn: str,
        **_kwargs
    ):
        super().__init__(scope, id)
        self.stream_name = stream_name

        self.function = aws_lambda.Function(
            self,
            id='sla_stream_function',
            code=aws_lambda.Code.from_asset(
                path='.',
                exclude=['cdk.out'],
                bundling={
                    # pylint: disable=no-member 
                    # bundling_docker_image is there.
                    'image': aws_lambda.Runtime.PYTHON_3_6.bundling_docker_image,
                    'command': [
                        'bash',
                        '-c',
                        'cp -r dataquality/ /asset-output/ && cp -r lambda/ /asset-output/ && cp -r definitions/ /asset-output/ && cp -r accounts/ /asset-output/'
                    ]
                }
            ),
            handler='lambda.sla_stream_producer.main',
            timeout=core.Duration.minutes(10),
            runtime=aws_lambda.Runtime.PYTHON_3_6,
            environment={
                'KINESIS_STREAM_NAME': self.stream_name,
                'ALARM_NAME_PREFIX': 'data-gov'
            }
        )

        get_policy = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            resources=['*'],
            actions=[
                'cloudwatch:DescribeAlarms'
            ]
        )
        self.function.add_to_role_policy(get_policy)

        #Resource specific policy
        resource_policy = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            resources=[
                stream_arn,
                f'arn:aws:kms:*:{core.Aws.ACCOUNT_ID}:*'
            ],
            actions=[
                'kinesis:PutRecords',
                'dynamodb:DescribeTable',
                'dynamodb:ListTagsOfResource',
                'dynamodb:GetItem',
                'dynamodb:Scan',
                'kms:Decrypt',
                'kms:DescribeKey',
                'kms:Encrypt',
                'kms:GenerateDataKey*',
                'kms:ReEncrypt'
            ]
        )
        self.function.add_to_role_policy(resource_policy)
