"""

## Stream Producer
Lambda function that will query all metrics for a given namespace
and send metric data for the last 60 seconds to a kinesis stream

"""
import os
import json
from typing import List
from datetime import timedelta, datetime, timezone
import dateutil.parser
from enum import Enum
import itertools

import boto3
from botocore.exceptions import ClientError

from dataquality.stream import MetricStream
from definitions.definition import Definition

CW_CLIENT = boto3.client('cloudwatch')
KINESIS_CLIENT = boto3.client('kinesis')

class StreamName(Enum):
    KINESIS_MINUTE_STREAM_NAME: str = 'minute'
    KINESIS_HOUR_STREAM_NAME: str = 'hour'
    KINESIS_DAY_STREAM_NAME: str = 'day'

def main(
    event: dict,
    context: dict
) -> None:
    """Lambda Handler."""

    account_number = context.invoked_function_arn.split(":")[4]
    definition = Definition(account=account_number)
    dataset_stream = MetricStream(metric_sets=definition.metric_sets)

    end_time_exact = datetime.utcnow()
    end_time = end_time_exact - timedelta(minutes=end_time_exact.minute % 10,
        seconds=end_time_exact.second,
        microseconds=end_time_exact.microsecond)

    md_queries = dataset_stream.metric_data_queries(
            frequency=event['frequency']
        )

    if len(md_queries) <= 0:
        print(f"No metrics matched for {event['frequency']} frequency.")
        return False
    else:
        print("Matched metrics:")
        print(md_queries)

        # Group metrics by Period
        grouped_dict = {}
        for i in md_queries:
            k = i["MetricStat"]["Period"]
            if (grouped_dict.get(k) == None):
                grouped_dict[k]=[i]
            else:
                grouped_dict[k].append(i)

    # For each Period in grouped_dict get metric data and append to metrics_data list
    metrics_data = []

    for period, md in grouped_dict.items():
        start_time = end_time - timedelta(seconds=period)
        period_metrics_data = get_metric_data(
            metric_data_queries=md,
            start_time=start_time,
            end_time=end_time
        )
        for metric_object in period_metrics_data:
            metrics_data.append(metric_object)
            print(metric_object)

    put_metrics(
        metrics_data=metrics_data,
        time=end_time,
        event=event,
        context=context,
        metric_sets=dataset_stream.metrics
    )

def get_metric_data(metric_data_queries, start_time, end_time):
    """Paginate and return all metric data under namspace."""
    metric_data_results = []
    paginator = CW_CLIENT.get_paginator('get_metric_data')
    page_iterator = paginator.paginate(
        MetricDataQueries=metric_data_queries,
        StartTime=start_time,
        EndTime=end_time
    )
    for page in page_iterator:
        metric_data_results += page['MetricDataResults']
    return metric_data_results

def translate_metrics_to_records(metrics_data: List[dict], time: datetime, event: dict, context: dict, metric_sets):
    """Translate CW metrics list to Kinesis stream records."""
    records = []
    metadata_map = {}
    dimensions_map = {}

    for metric_object in metrics_data:
        for metric in metric_sets:
            if metric.unique_id() == metric_object['Id']:
                metric_object['Namespace'] = metric.namespace
                metric_object['Name'] = metric.name
                metric_object['Period'] = metric.period
                metric_object['Statistic'] = metric.statistic
                if metric.metadata:
                    for meta in metric.metadata:
                        metadata_map[meta.name] = meta.value
                    metric_object['Metadata'] = metadata_map
                if metric.dimensions:
                    for dimension in metric.dimensions:
                        dimensions_map[dimension.name] = dimension.value
                    metric_object['Dimensions'] = dimensions_map
            else:
                continue

        metric_object['CollectionTime'] = time.replace(tzinfo=timezone.utc).isoformat()
        metric_object['AccountId'] = context.invoked_function_arn.split(":")[4]
        metric_object['Region'] = context.invoked_function_arn.split(":")[3]
        metric_object['MetricTimestamp'] = metric_object['Timestamps'][0] if len(metric_object['Timestamps']) > 0 else None
        metric_object['MetricValue'] = metric_object['Values'][0] if len(metric_object['Values']) > 0 else None
        metric_object['Frequency'] = event['frequency']
        records.append({
            'Data': json.dumps(metric_object, default=str),
            'PartitionKey': 'default'
        })
    print(records)
    return records

def put_metrics(metrics_data: List[dict], time: datetime, event: dict, context: dict, metric_sets):
    """Put records to kinesis stream"""
    try:
        KINESIS_CLIENT.put_records(
            Records=translate_metrics_to_records(
                metrics_data=metrics_data,
                time=time,
                event=event,
                context=context,
                metric_sets=metric_sets
            ),
            StreamName=os.environ[StreamName(event['frequency']).name]
        )
    except ClientError as ex:
        raise ex

def frequency_to_period(frequency: str) -> int:
    """ Convert rate string to period in seconds."""
    if frequency == "day":
        period = 86400
    if frequency == "minute":
        period = 60
    if frequency == "hour":
        period = 3600
    return period
