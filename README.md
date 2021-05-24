# Welcome to Dataset Ingestion Metrics Collection Framework.    

***   

### Understanding the directory structure with respect to the root of the repository:

| DirectoryName | Description | 
| --- | --- |
|  `accounts/`  |  Stores the accounts landscape for the application together with supporting functions.  |
|  `cdk_constructs/`  |  This directory holds all the L3 constructs which will be leveraged in the CDK Stacks.  | 
|  `dataquality/` |  This directory holds the modules which will be leveraged to generate Metrics and Alarms.  |
|  `definitions/`  |  The metrics which needs to be scraped and alarms which needs to be generated should be declared under this directory across accounts.  | 
|  `glue/`  |  Stores the dependency files for Glue jobs, including scripts, jars, etc.  |
|  `lambda/`  |  Stores the dependency files for Lambda functions.  |
|  `stacks/`  |  This directory holds the CDK Stacks, which would be instantited from app.py file.  |

***   

### Understanding a sample Metric definition under `definitions/account/` in detail:   

```
metric_set = MetricSet("sample")

metric = Metric(
        metric_set: Metricset object,
        namespace: string,
        name: string,
        frequency: enum,
        statistic: string,
        dashboard: dashboard object
        metadata: Metadata object
        dimensions: Dimensions object
    )
```

***   

### Properties     

***   

#### metric_set   

MetricSet can be instantiated with the group of metrics which belongs to a data-set. The assigned object should be provided to the key `metric_set`

***Required***: `Yes`    

***   

#### namespace  

`namespace` maps to the AWS CloudWatch Namespace which is either availabe by default or created by a user(custom namespace).   

***Required***: `Yes`      

***   

#### name  

`name` maps to the AWS CloudWatch Metric name which is either availabe as a default or a custom metric.      
***Required***: `Yes`   

***   

#### frequency     
`frequency` helps a user declare the frequency at which the declared metric would be scraped and streamed by the `metric-streamer` lambda.      
***Required***: `Yes`   

***   

#### statistic    

`statistic` helps a user declare a valid statistic from the [supporrted](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_concepts.html#Statistic) statistics.
***Required***: `Yes`   

***   

#### dashboard     
`dashboard` helps a user create an AWS CloudWatch dashboard providing the `dashboard_name` and `dashboard_category`. Within AWS CloudWatch dashboards, there is no functionality which provides categorization/relationship between different dashboards and in order to bridge that gap, `dashboard_category` can be used to determine the category type and add relevant dashboards using `dashboard_name` under it.   
> dashboard_name can be used to map to a data-set (if applies)    
> dashboard_category can be used to map to a data-source (if applies)   

***Required***: `Yes`   

***   

#### metadata   

`metadata` helps a user declare custom meta-data that could be streamed which helps from the reporting/querying perspective.   

***Required***: `Optional`   

***   

#### dimensions    

`dimensions` helps a user declare the Dimesions under which AWS CloudWatch metric isn available under.      

***Required***: `Optional`   

***   

### Understanding a sample SLA definition under `definitions/account/` in detail:   

```
sla_set = SLASet()

sla = SLA(
    sla_set: sla_set object
    metric: metric object,
    threshold: int,
    comparison_operator: string,
    details: str,
    short_description: str,
    severity: str,
    central_sns_enabled: bool
)
```

***   

## Properties     

***   

#### sla_set   

SLASet can be instantiated with the group of SLAs which belongs to a data-set. The assigned object should be provided to the key `sla_set`

***Required***: `Yes`    

***   

#### threshold  

`threshold` maps to the AWS CloudWatch MetricAlarm's attribute [Threshold](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricAlarm.html), which decides the CloudWatch alarming action relative to the datapoints received.   

***Required***: `Yes`      

***   

#### comparison_operator  

`comparison_operator` maps to the AWS CloudWatch MetricAlarm's attribute [ComparisonOperator](https://docs.aws.amazon.com/AmazonCloudWatch/latest/APIReference/API_MetricAlarm.html), which is used when comparing the specified statistic and threshold.      
***Required***: `Yes`   

***   

#### severity     
`severity` helps a user declare the custom severity.          
***Required***: `Optional`   

***   

#### details    

`details` helps a user declare what details should be published to the SIM/TT, when the threshold is breached.   
> Example: Location to the playbook that needs to be followed when the threshold is breached.   
***Required***: `Yes`   

***   

#### short_description     
`short_description` helps a user declare description about the breached activity, which would be published to the SIM/TT.   
   
***Required***: `Yes`   

#### central_sns_enabled     
`central_sns_enabled` helps a user to control alarm events flow to the central sns topic.   
   
***Required***: `Optional`   

***   

## Examples   

### How to define a metric definition?     
```
metric_set = MetricSet("dataset-1")
dashboard = Widget(dashboard_name='dataset-1', dashboard_category='data-project')

test_metric = Metric(
        metric_set=metric_set,
        namespace='AWS/Lambda',
        name='Invocations',
        frequency=Metric.DAY,
        statistic='Average',
        dashboard=dashboard,
        metadata=[
            Metadata(
                name='Account',
                value='Ingest'
            ),
            Metadata(
                name='Dataset',
                value='dataset-1'
            )
        ],
        dimensions=[
            Dimension(
                name='FunctionName',
                value='hello-world'
            )
        ]
    )
```

### How to define an SLA definition?     
```
sla_set = SLASet()

sla = SLA(
    sla_set = sla_set,
    metric= test_metric,
    threshold=1,
    comparison_operator="LESS_THAN_OR_EQUAL_TO_THRESHOLD",
    details = 'What details should i let a user when the SLA is breached?',
    short_description = 'Short description about the breaching activity',
    severity = "SEV_4",
    central_sns_enabled = True
)

```

Security
See CONTRIBUTING for more information.

License
This library is licensed under the MIT-0 License. See the LICENSE file.