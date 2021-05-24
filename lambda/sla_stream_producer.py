"""

## Stream Producer
Lambda function that will query all metrics for a given namespace
and send metric data for the last 60 seconds to a kinesis stream

"""
import os
import json
from typing import List
from datetime import timedelta, datetime, timezone

import boto3

from definitions.definition import Definition
from dataquality.stream import MetricStream

CW_CLIENT = boto3.client('cloudwatch')
KINESIS_CLIENT = boto3.client('kinesis')
KINESIS_STREAM_NAME = os.environ['KINESIS_STREAM_NAME']
ALARM_NAME_PREFIX = os.environ['ALARM_NAME_PREFIX']

def main(
    event: dict,
    context: dict
) -> None:
    """Lambda Handler."""

    account_number = context.invoked_function_arn.split(":")[4]
    definition = Definition(account=account_number)
    dataset_stream = MetricStream(metric_sets=definition.metric_sets)
    metric_sets=dataset_stream.metrics

    time = datetime.now()
    sla_data = get_sla_data(
        alarmNamePrefix=ALARM_NAME_PREFIX
    )

    for sla_object in sla_data:
        print(sla_object)

    put_slas(
        slas_data=sla_data,
        time=time,
        event=event,
        context=context,
        metric_sets=metric_sets
    )

def get_sla_data(alarmNamePrefix):
    """Paginate and return all metric data under namspace."""
    sla_data_results = []
    paginator = CW_CLIENT.get_paginator('describe_alarms')
    page_iterator = paginator.paginate(
        AlarmNamePrefix=alarmNamePrefix
    )
    for page in page_iterator:
        sla_data_results += page['MetricAlarms']
    return sla_data_results

def translate_clas_to_records(slas_data: List[dict], time: datetime, event: dict, context: dict, metric_sets):
    """Translate CW sla list to Kinesis stream records."""
    records = []
    metadata_map = {}

    for sla_object in slas_data:
        for metric in metric_sets:
            resolved_alarm_id_from_cloudwatch = ("-".join(sla_object['AlarmName'].split('-')[3:-5]))
            resolved_alarm_id_from_metric_object = str(metric.alarm_unique_id())[:-1]
            if resolved_alarm_id_from_metric_object == resolved_alarm_id_from_cloudwatch:
                sla_object['AccountId'] = context.invoked_function_arn.split(":")[4]
                sla_object['Region'] = context.invoked_function_arn.split(":")[3]
                sla_object['MetricNamespace'] = sla_object['Namespace']
                sla_object['MetricPeriod'] = sla_object['Period']
                sla_object['MetricStatistic'] = sla_object['Statistic']
                sla_object['CollectionTime'] = time.replace(tzinfo=timezone.utc).isoformat()
                if metric.metadata:
                    for meta in metric.metadata:
                        metadata_map[meta.name] = meta.value
                    sla_object['Metadata'] = metadata_map
            else:
                continue
        records.append({
            'Data': json.dumps(sla_object, default=str),
            'PartitionKey': 'default'
        })
    print(records)
    return records

def put_slas(slas_data: List[dict], time: datetime, event: dict, context: dict, metric_sets):
    """Put records to kinesis stream"""
    KINESIS_CLIENT.put_records(
        Records=translate_clas_to_records(
            slas_data=slas_data,
            time=time,
            event=event,
            context=context,
            metric_sets=metric_sets
        ),
        StreamName=KINESIS_STREAM_NAME
    )
