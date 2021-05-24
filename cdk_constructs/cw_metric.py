""" CloudWatch Alarm Construct """
from aws_cdk import (
    core,
    aws_cloudwatch
)

from dataquality.sla import SLA
from dataquality.metric import Metric

class CwMetric(core.Construct):
    """ CloudWatch Alarm Construct """
    sla: SLA

    def __init__(
        self,
        scope: core.Construct,
        id: str, # pylint: disable=redefined-builtin
        metric: Metric = None,
        sla: SLA = None,
        **_kwargs,
    ) -> None:
        super().__init__(scope, id)
        self.sla = sla
        self.metric = metric

        if self.sla:
            dimensions = {}
            if sla.metric.dimensions:
                for dimension in sla.metric.dimensions:
                    dimension_name = dimension.name
                    dimension_value = dimension.value
                    dimensions[dimension_name] = dimension_value

            # CDK Metric
            self.cw_metric = aws_cloudwatch.Metric(
                    namespace=self.sla.metric.namespace,
                    metric_name=self.sla.metric.name,
                    dimensions=dimensions
                )
        else:
            dimensions = {}
            if metric.dimensions:
                for dimension in metric.dimensions:
                    dimension_name = dimension.name
                    dimension_value = dimension.value
                    dimensions[dimension_name] = dimension_value

            # CDK Metric
            self.cw_metric = aws_cloudwatch.Metric(
                    namespace=self.metric.namespace,
                    metric_name=self.metric.name,
                    dimensions=dimensions
                )
