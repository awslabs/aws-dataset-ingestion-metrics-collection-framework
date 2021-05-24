""" Parse SLA Lambda Construct """
from aws_cdk import (
    core,
    aws_iam,
    aws_lambda
)

class SlaParseConstruct(core.Construct):
    """ Lambda Construct """
    stream_name: str

    def __init__(
        self,
        scope: core.Construct,
        id: str, # pylint: disable=redefined-builtin
        central_account_number: int,
        central_sns_topic: str,
        **_kwargs
    ):
        super().__init__(scope, id)
        self.central_sns_topic = central_sns_topic
        self.central_account_number = central_account_number
        self.function = aws_lambda.Function(
            self,
            id='rewrite_function',
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
            handler='lambda.sla_parse.main',
            timeout=core.Duration.minutes(10),
            runtime=aws_lambda.Runtime.PYTHON_3_6,
            environment={
                'CENTRAL_SNS_TOPIC': self.central_sns_topic,
                'CENTRAL_ACCOUNT_NUMBER': self.central_account_number
            }
        )

        #Resource specific policy
        resource_policy = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            resources=[
                f'arn:aws:sns:*:{self.central_account_number}:{self.central_sns_topic}',
                f'arn:aws:kms:*:{core.Aws.ACCOUNT_ID}:*'
            ],
            actions=[
                'sns:Publish',
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
