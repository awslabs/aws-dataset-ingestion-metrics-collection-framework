import boto3
import datetime
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.job import Job
from dataquality.metric import *
from definitions.definition import Definition

args = getResolvedOptions(sys.argv, ['account_number','metric_set_name'])

# Session and Context Initialization
spark = (SparkSession
    .builder
    .config("spark.sql.crossJoin.enabled", "true")
    .getOrCreate())
glueContext = GlueContext(spark.sparkContext.getOrCreate())
client = boto3.client('cloudwatch')

account_number = args['account_number']
definition = Definition(account=account_number)
for metric_set in definition.metric_sets:
    if metric_set.name == args['metric_set_name']:
        break

# Dataset collection
datasets = []
for metric in metric_set.metrics:
    if isinstance(metric, BusinessMetric) and metric.dataset not in datasets:
        datasets.append(metric.dataset)

# Dataframes load
for dataset in datasets:
    for metric in metric_set.metrics:
        if metric.dataset == dataset:
            result_df=spark.sql(metric.query)
            result_value=result_df.collect()[0][0]
        
            dimensions=[]
            for dimension in metric.dimensions:
                dimensions.append(dimension.api_structure())

            if result_value != None:
                client.put_metric_data(
                    Namespace=metric.namespace,
                    MetricData=[
                        {
                            'MetricName': metric.name,
                            'Dimensions': dimensions,
                            'Timestamp': datetime.datetime.utcnow(),
                            'Value': result_value
                        },
                    ]
                )
