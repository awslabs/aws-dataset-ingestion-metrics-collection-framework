""" Metric Definitions """

from dataquality.metric import Metric, Dimension, Metadata, Widget
from dataquality.sla import SLA
from dataquality.set import MetricSet, SLASet

dashboard = Widget(dashboard_name='test_dashboard_category')

metric_set = MetricSet("test_category")
sla_set = SLASet()

test_metric = Metric(
        metric_set=metric_set,
        namespace='AWS/Lambda',
        name='Invocations',
        frequency=Metric.DAY,
        dashboard=dashboard,
        statistic='Sum',
        metadata=[ # Optional
            Metadata(
                name='Thing',
                value='foobar'
            )
        ],
        dimensions=[ # Optional
            Dimension(
                name='FunctionName',
                value='hello_world'
            )
        ]
    )

test_sla = SLA(
    sla_set = sla_set,
    metric= test_metric,
    threshold=1,
    comparison_operator="LESS_THAN_OR_EQUAL_TO_THRESHOLD",
    severity = "SEV 5",
    details = 'test_sla',
    short_description = 'test_sla',
    sns_enabled=True
)
