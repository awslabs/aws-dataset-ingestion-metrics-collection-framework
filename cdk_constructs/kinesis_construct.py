""" Kinesis Stream and Firehose Construct """
import os
from aws_cdk import (
    core,
    aws_iam,
    aws_kinesis,
    aws_kinesisfirehose,
    aws_logs
)
from accounts.accounts import fetch_account_central
ACCOUNT_NUMBER = os.environ.get('CDK_DEPLOY_ACCOUNT')
CENTRAL_ACCOUNT_NUMBER = fetch_account_central(ACCOUNT_NUMBER)

class KinesisConstruct(core.Construct):
    """ Kinesis Stream and Firehose Construct """
    bucket_arn: str
    prefix: str
    errorOutputPrefix: str

    def __init__(
        self,
        scope: core.Construct,
        id: str, # pylint: disable=redefined-builtin
        bucket_arn: str,
        prefix: str,
        errorOutputPrefix: str,
        database: str,
        table: str,
        **_kwargs
    ):
        super().__init__(scope, id)
        self.bucket_arn = bucket_arn
        self.prefix = prefix
        self.errorOutputPrefix = errorOutputPrefix
        self.database = database
        self.table = table

        #Kinesis Stream
        self.kinesis_stream = aws_kinesis.Stream(
                self,
                id='stream'
        )

        log_group = aws_logs.LogGroup(
            self, 
            id="FirehoseLogGroup",
            removal_policy=core.RemovalPolicy.DESTROY
        )

        log_stream = aws_logs.LogStream(
            self,
            id="FirehoseLogStream",
            log_group=log_group,
            removal_policy=core.RemovalPolicy.DESTROY
        )

        role_firehose_policy = aws_iam.PolicyDocument(statements=[
            aws_iam.PolicyStatement(
                actions=[
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                    "s3:PutObjectAcl",
                    "kinesis:DescribeStream",
                    "glue:Get*",
                    "glue:Put*",
                    "glue:Update*",
                    "logs:PutLogEvents"
                ],
                resources=[
                    self.bucket_arn,
                    "{}/*".format(self.bucket_arn),
                    'arn:aws:kinesis:*:*:stream/data-gov*',
                    f'arn:aws:glue:*:{core.Aws.ACCOUNT_ID}:catalog',
                    f'arn:aws:glue:*:{core.Aws.ACCOUNT_ID}:database/*',
                    f'arn:aws:glue:*:{core.Aws.ACCOUNT_ID}:table/*',
                    f"arn:aws:logs:*:{core.Aws.ACCOUNT_ID}:log-group:{log_group.log_group_name}:log-stream:{log_stream.log_stream_name}"
                ]
            ),
            aws_iam.PolicyStatement(
                effect=aws_iam.Effect.ALLOW,
                actions=[
                        "kms:Encrypt",
                        "kms:Decrypt",
                        "kms:ReEncrypt*",
                        "kms:GenerateDataKey*",
                        "kms:DescribeKey"
                    ],
                resources=[f"arn:aws:kms:*:{CENTRAL_ACCOUNT_NUMBER}:key/*"]
            )
        ])

        role_firehose = aws_iam.Role(
            self, "FirehoseRole",
            assumed_by=aws_iam.ServicePrincipal("firehose.amazonaws.com"),
            inline_policies={
                "FirehosePolicy": role_firehose_policy
            }
        )

        self.kinesis_stream.grant_read(role_firehose)

        #Kinesis Firehose
        aws_kinesisfirehose.CfnDeliveryStream(
            self,
            id="DeliveryFirhose",
            delivery_stream_type="KinesisStreamAsSource",
            kinesis_stream_source_configuration={
                "kinesisStreamArn": self.kinesis_stream.stream_arn,
                "roleArn": role_firehose.role_arn,
            },
            extended_s3_destination_configuration={
                "bucketArn": self.bucket_arn,
                "prefix": self.prefix,
                "errorOutputPrefix": self.errorOutputPrefix,
                "roleArn": role_firehose.role_arn,
                "compressionFormat": "UNCOMPRESSED",
                "bufferingHints": {
                    "intervalInSeconds": 60,
                    "sizeInMBs": 64,
                },
                "dataFormatConversionConfiguration": {
                    "enabled" : True,
                    "inputFormatConfiguration" : {
                        "deserializer": {
                            "openXJsonSerDe": {
                            }
                        }
                    },
                    "outputFormatConfiguration" : {
                        "serializer" : {
                            "parquetSerDe" : {}
                        }
                    },
                    "schemaConfiguration" : {
                        "catalogId": core.Aws.ACCOUNT_ID,
                        "databaseName": self.database,
                        "tableName": self.table,
                        "roleArn": role_firehose.role_arn,
                        "region": core.Aws.REGION,
                        "versionId": "LATEST"
                    }
                },
                "cloudWatchLoggingOptions": {
                    "enabled" : True,
                    "logGroupName" : log_group.log_group_name,
                    "logStreamName" : log_stream.log_stream_name
                }
            }
        )