""" Lambda Construct """
from aws_cdk import (
    core,
    aws_iam,
    aws_lambda
)

class MetricStreamerConstruct(core.Construct):
    """ Lambda Construct """

    def __init__(
        self,
        scope: core.Construct,
        id: str, # pylint: disable=redefined-builtin
        stream_dict: dict,
        **_kwargs
    ):
        super().__init__(scope, id)
        self.stream_dict = stream_dict
        self.stream_names = {}
        self.stream_arns = []
        for key, value in self.stream_dict.items():
            if key.endswith("STREAM_NAME") == True:
                self.stream_names[key]=value
            if key.endswith("STREAM_ARN") == True:
                self.stream_arns.append(value)

        self.function = aws_lambda.Function(
            self,
            id='stream_function',
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
            handler='lambda.metric_stream_producer.main',
            timeout=core.Duration.minutes(10),
            runtime=aws_lambda.Runtime.PYTHON_3_6,
            environment=self.stream_names
        )

        get_policy = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            resources=['*'],
            actions=[
                'cloudwatch:GetMetricData'
            ]
        )
        self.function.add_to_role_policy(get_policy)

        #Resource specific policy
        resource_policy = aws_iam.PolicyStatement(
            effect=aws_iam.Effect.ALLOW,
            resources=self.stream_arns + [
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