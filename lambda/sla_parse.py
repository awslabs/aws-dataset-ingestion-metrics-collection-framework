"""
## Parse SLAsets
Lambda function that will parse the SLA objects and
establish the mapping between the Invoked CW Alarm and Alarm obtained from SLAset.
"""
import os
import json
import boto3

from definitions.definition import Definition

SNS_CLIENT = boto3.client('sns')
CENTRAL_SNS_TOPIC = os.environ['CENTRAL_SNS_TOPIC']
CENTRAL_ACCOUNT_NUMBER = os.environ['CENTRAL_ACCOUNT_NUMBER']

def main(
    event: dict,
    context: dict
) -> None:
    """Lambda Handler."""

    alarm_name = json.loads(event['Records'][0]['Sns']['Message'])['AlarmName']
    invoked_state = (event['Records'][0]['Sns']['Subject']).split(':')[0]
    print("CloudWatch Alarm Name from the SNS topic payload is :{}".format(alarm_name))

    derived_list = []
    derived_list.append(alarm_name[0:alarm_name.find('-SLA')].split('-')[3])
    derived_list.append(alarm_name[0:alarm_name.find('-SLA')].split('-')[4])
    derived_list.append("-".join((alarm_name[0:alarm_name.find('-SLA')].split('-')[6:])))
    print("The derived list used for future validation is : {}".format(derived_list))

    account_number = context.invoked_function_arn.split(":")[4]
    region = event['Records'][0]['EventSubscriptionArn'].split(':')[3]
    definition = Definition(account=account_number)

    for sla_set in definition.sla_sets:
        for sla in sla_set.slas:

            metric_name = sla.metric.name
            frequency = sla.metric.frequency
            for dimension in sla.metric.dimensions:
                if str(dimension.name).endswith('Bucket'):
                    continue
                dimension_value = dimension.value

            if all(x in derived_list for x in [metric_name.lower(), frequency, dimension_value.lower()]):
                print("metric_name: {}, frequency: {} and dimension_value: {} exist in the dervied_list".format(
                    metric_name.lower(),
                    frequency,
                    dimension_value)
                )

                try:
                    details = sla.details
                    short_description = sla.short_description
                    impact = sla.severity
                except AttributeError as ex:
                    print("Failed to grab the required attributes from the Metric definitions with error: {}".format(ex))

                reference_id = "Unknown"
                for metadata in sla.metric.metadata:
                    if metadata.name.lower() == "function" or metadata.name.lower() == "dataset":
                        reference_id = metadata.value

                # Add fields as needed to the payload which will be published to the central sns topic
                payload = {
                    "details" : details,
                    "short_description": short_description + ' caused by CloudWatch Alarm in ' + invoked_state + ' state',
                    "impact" : impact,
                    "unique_id": dimension_value+'-'+metric_name+'-'+frequency,
                    "alarm_origin": "Data Governance",
                    "reference_id": reference_id
                }

                if sla.sns_enabled:
                    #Send the payload from the SLA object
                    print("sending the payload to the central SNS topic")
                    write_to_sns(payload, region)
                else:
                    print(
                        "Could not find tt_create attribute in the metric definition, hence logging the value of details: {}, short_description: {}".format(
                            details,
                            short_description
                        )
                    )
            else:
                raise ValueError("Could not find the metric_name and dimension_value in dervied_list")

def write_to_sns(payload, region):
    """ Publish to SNS Topic. """
    SNS_CLIENT.publish(
        TopicArn=f'arn:aws:sns:{region}:{CENTRAL_ACCOUNT_NUMBER}:{CENTRAL_SNS_TOPIC}',
        Message=json.dumps(payload)
    )
    print("Published the payload to the central SNS topic successfully")
