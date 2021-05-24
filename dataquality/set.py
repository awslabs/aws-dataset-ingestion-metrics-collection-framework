"""Data Governance Metric Definitions"""
from typing import (
    List,
    Union
)
from .metric import (
    Metric,
    BusinessMetric
)
from .sla import SLA

class MetricSet():
    """Metric Set"""
    name: str
    metrics: List[Union[Metric,BusinessMetric]]
    schedule: str

    def __init__(
        self,
        name: str,
        metrics: List[Union[Metric,BusinessMetric]] = (),
        schedule: str = None
    ) -> None:
        self.name = name
        self.metrics = metrics
        self.schedule = schedule

    def add(self, metric: Metric):
        """ Add metric. """
        self.metrics = self.metrics + (metric,)

class BusinessMetricSet(MetricSet):
    """Business Metric Set"""
    metrics: List[BusinessMetric]

class SLASet():
    """SLA Set"""
    slas: List[SLA]

    def __init__(
        self,
        slas: List[SLA] = ()
    ) -> None:
        self.slas = slas

    def add(self, sla: SLA):
        """ Add SLA. """
        self.slas = self.slas + (sla,)
