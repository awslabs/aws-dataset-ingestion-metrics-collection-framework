"""DGM"""
from typing import (
    List,
    Dict
)
from re import sub
from .dataset import Dataset

class Dimension():
    """Metric Dimension"""
    name: str
    value: str

    def __init__(self, name: str, value: str) -> None:
        self.name = name
        self.value = value

    def api_structure(self) -> dict:
        """Return in boto3 API structure."""

        return {
            'Name': self.name,
            'Value': self.value
        }

class Widget():
    """
    Declare dashboard name for the metric to be available
    Declare dashboard_category for the use-case specific dashboard to be grouped in
    """
    dashboard_name: str
    dashboard_category: str
    def __init__(
        self,
        dashboard_name: str,
        dashboard_category: str = None
    )-> None:
        self.dashboard_name = dashboard_name
        self.dashboard_category = dashboard_category

class Metadata():
    """Metric Metadata"""
    name: str
    value: str

    def __init__(self, name: str, value: str) -> None:
        self.name = name
        self.value = value

class Metric():
    """Metric"""
    namespace: str
    name: str
    frequency: str
    statistic: str
    period: int
    metadata: List[Metadata]
    dimensions: List[Dimension]
    dashboard: Widget

    DAY = 'day'
    HOUR = 'hour'
    MINUTE = 'minute'

    def __init__(
        self,
        namespace: str,
        name: str,
        frequency: str,
        statistic: str,
        dashboard: Widget,
        metric_set,
        sla_set = None,
        period: int = None,
        metadata: List[Metadata] = None,
        dimensions: List[Dimension] = None
    ) -> None:

        self.namespace = namespace
        self.name = name
        self.frequency = frequency
        self.period = period if period is not None else self.frequency_to_period(frequency)
        self.statistic = statistic
        self.metadata = metadata
        self.dimensions = dimensions
        self.metric_set = metric_set
        self.sla_set = sla_set
        self.dashboard = dashboard

        self.metric_set.add(self)

    @staticmethod
    def frequency_to_period(frequency: str) -> int:
        """ Convert rate string to period in seconds."""
        if frequency == Metric.DAY:
            period = 86400
        if frequency == Metric.MINUTE:
            period = 60
        if frequency == Metric.HOUR:
            period = 3600
        return period

    def api_structure(self) -> dict:
        """Return in boto3 API structure."""

        dimensions = []
        if self.dimensions:
            for dimension in self.dimensions:
                dimensions.append(dimension.api_structure())

        return {
            'Namespace': self.namespace,
            'MetricName': self.name,
            'Dimensions': dimensions

        }

    def widget_title(self) -> str:
        """Generate title for the CloudWatch Widgets"""

        metric_id = self.name + ' per ' + self.frequency + '-'

        if self.dimensions:
            for dimension in self.dimensions:
                if str(dimension.name).endswith('Bucket'):
                    continue
                metric_id += dimension.value

        return metric_id.replace('/', '').lower()

    def alarm_unique_id(self) -> str:
        """Generate short ID for AlarmName creation"""

        metric_id = self.namespace + '-' + self.name + '-' + self.frequency + '-'

        if self.dimensions:
            for dimension in self.dimensions:
                if str(dimension.name).endswith('Bucket'):
                    continue
                metric_id += dimension.name + '-' + dimension.value + '-'

        return metric_id.replace('/', '').lower()

    def unique_id(self) -> str:
        """Generate short ID."""

        metric_id = self.namespace + self.name + self.frequency

        if self.dimensions:
            for dimension in self.dimensions:
                if str(dimension.name).endswith('Bucket'):
                    continue
                metric_id += dimension.name + dimension.value

        return sub(r'\W+', '', metric_id).lower()

class DataSetMetric(Metric):
    """DataSetMetric"""
    dataset: Dataset
    def __init__(
        self,
        dataset: Dataset,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.dataset = dataset

class BusinessMetric(DataSetMetric):
    """BusinessMetric"""
    query: str
    reference_datasets: List[Dataset]
    def __init__(
        self,
        query: str,
        reference_datasets: List[Dataset],
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.reference_datasets = reference_datasets
        self.query = query