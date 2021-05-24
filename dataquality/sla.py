""" SLA """
from dataquality.metric import Metric

class SLA():
    """ SLA """
    metric: Metric

    def __init__(
        self,
        sla_set,
        metric: Metric,
        short_description: str,
        details: str,
        threshold: int,
        comparison_operator: str,
        treat_missing_data: str = "NOT_BREACHING",
        severity: str = "default",
        datapoints_to_alarm: int = 1,
        evaluation_periods: int = 1,
        sns_enabled: bool = False
    ) -> None:
        self.sla_set = sla_set
        self.metric = metric
        self.threshold = threshold
        self.comparison_operator = comparison_operator
        self.datapoints_to_alarm = datapoints_to_alarm
        self.evaluation_periods = evaluation_periods
        self.treat_missing_data = treat_missing_data
        self.short_description = short_description
        self.details = details
        self.sla_set.add(self)
        self.severity = severity
        self.sns_enabled = sns_enabled