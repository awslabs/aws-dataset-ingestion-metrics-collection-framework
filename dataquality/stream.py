"""Stream"""
from typing import List
from .set import (
    MetricSet
)

class MetricStream():
    """
    Stream a metric set
    """
    metric_sets: List[MetricSet]

    def __init__(
        self,
        metric_sets: List[MetricSet]
    ) -> None:
        self.metric_sets = metric_sets
        self.metrics = []

        # Flatten metrics into single list
        for metric_set in self.metric_sets:
            self.metrics += metric_set.metrics

    def metric_data_queries(self, frequency) -> list:
        """Return MetricDataQueries"""

        metric_data_queries = []

        for metric in self.metrics:

            if metric.frequency != frequency:
                continue

            metric_data_query = {
                'Id': metric.unique_id(),
                'MetricStat': {
                    'Metric': metric.api_structure(),
                    'Period': metric.period,
                    'Stat': metric.statistic

                }
            }
            metric_data_queries.append(metric_data_query)

        return metric_data_queries
