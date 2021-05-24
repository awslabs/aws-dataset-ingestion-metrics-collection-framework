""" CloudWatch Alarm Construct """
from aws_cdk import (
    core,
    aws_cloudwatch,
    aws_cloudwatch_actions,
    aws_sns
)

from dataquality.sla import SLA
from .cw_metric import CwMetric

class Alarm(core.Construct):
    """ CloudWatch Alarm Construct """
    threshold: int
    sla: SLA
    datapoints_to_alarm: int
    evaluation_periods: int

    def __init__(
        self,
        scope: core.Construct,
        id: str, # pylint: disable=redefined-builtin
        sla: SLA,
        sns_topic: aws_sns.Topic,
        **_kwargs,
    ) -> None:
        super().__init__(scope, id)
        self.sla = sla
        self.cw_metric = CwMetric(self, id=self.sla.metric.unique_id(), sla=self.sla)
        self.sns_topic = sns_topic
        # Alarm
        self.alarm = aws_cloudwatch.Alarm(
                scope=self,
                id='SLA-Alarm',
                metric=self.cw_metric.cw_metric,
                alarm_name='data-gov-'+id +'SLA-Alarm-'+core.Aws.REGION,
                datapoints_to_alarm=self.sla.datapoints_to_alarm,
                evaluation_periods=self.sla.evaluation_periods,
                threshold=self.sla.threshold,
                statistic=self.sla.metric.statistic,
                period=core.Duration.seconds(self.sla.metric.period),
                treat_missing_data=getattr(
                    aws_cloudwatch.TreatMissingData,
                    self.sla.treat_missing_data
                ),
                comparison_operator=getattr(
                    aws_cloudwatch.ComparisonOperator,
                    self.sla.comparison_operator
                )
            )

        self.alarm.add_alarm_action(
            aws_cloudwatch_actions.SnsAction(
                self.sns_topic
            )
        )

        self.alarm.add_insufficient_data_action(
            aws_cloudwatch_actions.SnsAction(
                self.sns_topic
            )
        )
