""" Glue Construct. """
import os
from aws_cdk import (
    aws_glue,
    aws_s3,
    aws_iam,
    custom_resources,
    core,
)

ACCOUNT_NUMBER = os.environ.get('CDK_DEPLOY_ACCOUNT')

class GlueCatalogConstruct(core.Construct):
    """ Glue Catalog Construct. """
    def __init__(
        self,
        scope: core.Construct,
        id:str,
        bucket_name: str,
        metric_frequencies: list,
        cross_account: str = None,
        **kwargs
    ):
        super().__init__(scope, id, **kwargs)
        self.bucket_name = bucket_name
        self.bucket_arn = f'arn:aws:s3:::{self.bucket_name}'
        self.bucket = aws_s3.Bucket.from_bucket_arn(
            self,
            id='CentralizedMonitoringBucket',
            bucket_arn=self.bucket_arn
        )
        self.cross_account=cross_account
        self.metric_frequencies = metric_frequencies
        self.database = aws_glue.Database(
            self,
            id='DataGovernanceDatabase',
            database_name='data_governance'
        )

        for frequency in self.metric_frequencies:
            self.metric_frequency_table = aws_glue.CfnTable(
                self,
                id=f'Metric{frequency.capitalize()}Table',
                catalog_id=ACCOUNT_NUMBER, 
                database_name=self.database.database_name, 
                table_input=aws_glue.CfnTable.TableInputProperty(
                    name=f'metrics_{frequency}',
                    parameters={
                        "classification": "parquet",
                        "has_encrypted_data": "false"
                    },
                    partition_keys=[{
                        "name": "region",
                        "type": "string"
                    }, {
                        "name": "year",
                        "type": "smallint"
                    }, {
                        "name": "month",
                        "type": "smallint"
                    }, {
                        "name": "day",
                        "type": "smallint"
                    }, {
                        "name": "hour",
                        "type": "smallint"
                    }],
                    storage_descriptor=aws_glue.CfnTable.StorageDescriptorProperty(
                        columns=[{
                            "name": "collectiontime",
                            "type": "string"
                        }, {
                            "name": "namespace",
                            "type": "string"
                        }, {
                            "name": "name",
                            "type": "string"
                        }, {
                            "name": "period",
                            "type": "int"
                        }, {
                            "name": "frequency",
                            "type": "string"
                        }, {
                            "name": "statistic",
                            "type": "string"
                        }, {
                            "name": "metadata",
                            "type": "string"
                        }, {
                            "name": "dimensions",
                            "type": "string"
                        }, {
                            "name": "accountid",
                            "type": "string"
                        }, {
                            "name": "metrictimestamp",
                            "type": "string"
                        }, {
                            "name": "metricvalue",
                            "type": "float"
                        }, {
                            "name": "id",
                            "type": "string"
                        }, {
                            "name": "label",
                            "type": "string"
                        }], 
                        compressed=False, 
                        input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat', 
                        location=f's3://{bucket_name}/metrics/{frequency}/',
                        output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                        serde_info=aws_glue.CfnTable.SerdeInfoProperty(
                            parameters={
                                "serialization.format": "1"
                            },
                            serialization_library='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                        )
                    ),
                    table_type='EXTERNAL_TABLE'
                )
            )

        self.sla_table = aws_glue.CfnTable(
            self,
            id='SLATable',
            catalog_id=ACCOUNT_NUMBER, 
            database_name=self.database.database_name, 
            table_input=aws_glue.CfnTable.TableInputProperty(
                name='slas',
                parameters={
                    "classification": "parquet",
                    "has_encrypted_data": "false"
                },
                partition_keys=[{
                    "name": "region",
                    "type": "string"
                }, {
                    "name": "year",
                    "type": "smallint"
                }, {
                    "name": "month",
                    "type": "smallint"
                }, {
                    "name": "day",
                    "type": "smallint"
                }, {
                    "name": "hour",
                    "type": "smallint"
                }],
                storage_descriptor=aws_glue.CfnTable.StorageDescriptorProperty(
                    columns=[{
                        "name": "collectiontime",
                        "type": "string"
                    }, {
                        "name": "alarmarn",
                        "type": "string"
                    }, {
                        "name": "alarmname",
                        "type": "string"
                    }, {
                        "name": "metricnamespace",
                        "type": "string"
                    }, {
                        "name": "metricname",
                        "type": "string"
                    }, {
                        "name": "metricperiod",
                        "type": "int"
                    }, {
                        "name": "metricfrequency",
                        "type": "string"
                    }, {
                        "name": "metricstatistic",
                        "type": "string"
                    }, {
                        "name": "threshold",
                        "type": "float"
                    }, {
                        "name": "comparisonOperator",
                        "type": "string"
                    }, {
                        "name": "treatmissingdata",
                        "type": "string"
                    }, {
                        "name": "statevalue",
                        "type": "string"
                    }, {
                        "name": "statereason",
                        "type": "string"
                    }, {
                        "name": "accountid",
                        "type": "string"
                    }, {
                        "name": "metadata",
                        "type": "string"
                    }], 
                    compressed=False, 
                    input_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat', 
                    location=f's3://{bucket_name}/slas/',
                    output_format='org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    serde_info=aws_glue.CfnTable.SerdeInfoProperty(
                        parameters={
                            "serialization.format": "1"
                        },
                        serialization_library='org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    )
                ),
                table_type='EXTERNAL_TABLE'
            )
        )

        self.metric_defs_table = aws_glue.CfnTable(
            self,
            id='MetricDefsTable',
            catalog_id=ACCOUNT_NUMBER, 
            database_name=self.database.database_name, 
            table_input=aws_glue.CfnTable.TableInputProperty(
                name='metric_defs',
                parameters={
                    "jsonPath": "$[*]",
                    "classification": "json"
                },
                storage_descriptor=aws_glue.CfnTable.StorageDescriptorProperty(
                    columns=[{
                        "name": "namespace",
                        "type": "string"
                    },
                    {
                        "name": "name",
                        "type": "string"
                    },
                    {
                        "name": "frequency",
                        "type": "string"
                    },
                    {
                        "name": "period",
                        "type": "int"
                    },
                    {
                        "name": "statistic",
                        "type": "string"
                    },
                    {
                        "name": "metadata",
                        "type": "string"
                    },
                    {
                        "name": "dimensions",
                        "type": "string"
                    },
                    {
                        "name": "metric_set",
                        "type": "string"
                    },
                    {
                        "name": "sla_set",
                        "type": "string"
                    },
                    {
                        "name": "dashboard",
                        "type": "string"
                    },
                    {
                        "name": "account",
                        "type": "string"
                    },
                    {
                        "name": "dataset",
                        "type": "string",
                    },
                    {
                        "name": "reference_datasets",
                        "type": "string"
                    },
                    {
                        "name": "query",
                        "type": "string"
                    }], 
                    compressed=False, 
                    input_format='org.apache.hadoop.mapred.TextInputFormat', 
                    location=f's3://{bucket_name}/definitions/metrics/',
                    output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serde_info=aws_glue.CfnTable.SerdeInfoProperty(
                        parameters={
                            "paths": "account,dashboard,dataset,dimensions,frequency,metadata,metric_set,name,namespace,period,query,reference_datasets,sla_set,statistic",
                            "strip.outer.array": "true"
                        },
                        serialization_library='org.openx.data.jsonserde.JsonSerDe'
                    )
                ),
                table_type='EXTERNAL_TABLE'
            )
        )

        self.sla_defs_table = aws_glue.CfnTable(
            self,
            id='SLADefsTable',
            catalog_id=ACCOUNT_NUMBER, 
            database_name=self.database.database_name, 
            table_input=aws_glue.CfnTable.TableInputProperty(
                name='sla_defs',
                parameters={
                    "jsonPath": "$[*]",
                    "classification": "json"
                },
                storage_descriptor=aws_glue.CfnTable.StorageDescriptorProperty(
                    columns=[{
                        "name": "ticket",
                        "type": "string"
                    },
                    {
                        "name": "threshold",
                        "type": "int"
                    },
                    {
                        "name": "comparison_operator",
                        "type": "string"
                    },
                    {
                        "name": "datapoints_to_alarm",
                        "type": "int"
                    },
                    {
                        "name": "evaluation_periods",
                        "type": "int"
                    },
                    {
                        "name": "treat_missing_data",
                        "type": "string"
                    },
                    {
                        "name": "severity",
                        "type": "string"
                    },
                    {
                        "name": "short_description",
                        "type": "string"
                    },
                    {
                        "name": "details",
                        "type": "string"
                    },
                    {
                        "name": "metric_namespace",
                        "type": "string"
                    },
                    {
                        "name": "metric_name",
                        "type": "string"
                    },
                    {
                        "name": "metric_set",
                        "type": "string"
                    },
                    {
                        "name": "metric_metadata",
                        "type": "string"
                    }, {
                        "name": "metric_dimensions",
                        "type": "string"
                    },
                    {
                        "name": "account",
                        "type": "string"
                    }], 
                    compressed=False, 
                    input_format='org.apache.hadoop.mapred.TextInputFormat', 
                    location=f's3://{bucket_name}/definitions/slas/',
                    output_format='org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    serde_info=aws_glue.CfnTable.SerdeInfoProperty(
                        parameters={
                            "paths": "account,comparison_operator,datapoints_to_alarm,details,evaluation_periods,metric_name,metric_namespace,metric_set,severity,short_description,threshold,ticket,treat_missing_data",
                            "strip.outer.array": "true"
                        },
                        serialization_library='org.openx.data.jsonserde.JsonSerDe'
                    )
                ),
                table_type='EXTERNAL_TABLE'
            )
        )

        if cross_account:
            self.put_policy = custom_resources.AwsCustomResourcePolicy.from_statements(statements=[
                aws_iam.PolicyStatement(
                    effect=aws_iam.Effect.ALLOW,
                    actions=['glue:PutResourcePolicy'],
                    resources=['*']
                )
            ])

            self.put_resource_policy = custom_resources.AwsCustomResource(
                self, 
                "putResourcePolicy",
                on_update={
                    "service": "Glue",
                    "action": "putResourcePolicy",
                    "parameters": {
                        "PolicyInJson": 
                            """{
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                    "Principal": {
                                        "AWS": [
                                        "arn:aws:iam::"""+self.cross_account+""":root"
                                        ]
                                    },
                                    "Effect": "Allow",
                                    "Action": [
                                        "glue:Get*",
                                        "glue:BatchGet*",
                                        "glue:Put*",
                                        "glue:Create*"
                                    ],
                                    "Resource": [
                                        "arn:aws:glue:"""+core.Aws.REGION+":"+core.Aws.ACCOUNT_ID+""":*"
                                    ]
                                    }
                                ]
                            }"""
                    },
                    "physical_resource_id": custom_resources.PhysicalResourceId.of(id='dataGovCatalog')
                },
                policy=self.put_policy
            )
