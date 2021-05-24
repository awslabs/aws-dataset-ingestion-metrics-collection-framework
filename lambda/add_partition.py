import os
import json
import boto3

catalogs = os.environ['catalogs'].split(',')
glue_client = boto3.client('glue')

def main(
    event: dict,
    context: dict
) -> None:
    """Lambda Handler."""
    database='data_governance'
    key=event['Records'][0]['s3']['object']['key']
    bucket=event['Records'][0]['s3']['bucket']['name']
    print(event)

    # Metrics tables are separated by frequency
    if 'metrics/' in key:
        table=key.split('/')[0]+'_'+key.split('/')[1]
        region=key.split('/')[2]
        year=key.split('/')[3]
        month=key.split('/')[4]
        day=key.split('/')[5]
        hour=key.split('/')[6]
    # Single SLA table
    else:
        table=key.split('/')[0]
        region=key.split('/')[1]
        year=key.split('/')[2]
        month=key.split('/')[3]
        day=key.split('/')[4]
        hour=key.split('/')[5]

    table_response = glue_client.get_table(
        DatabaseName=database,
        Name=table
    )
    input_format = table_response['Table']['StorageDescriptor']['InputFormat']
    output_format = table_response['Table']['StorageDescriptor']['OutputFormat']
    table_location = table_response['Table']['StorageDescriptor']['Location']
    serde_info = table_response['Table']['StorageDescriptor']['SerdeInfo']
    
    input_dict = {
        'Values': [
            region, year, month, day, hour
        ],
        'StorageDescriptor': {
            'Location': f"{table_location}{region}/{year}/{month}/{day}/{hour}/",
            'InputFormat': input_format,
            'OutputFormat': output_format,
            'SerdeInfo': serde_info
        }
    }
    
    for catalog_id in catalogs:
        partitions = glue_client.get_partitions(
            CatalogId=catalog_id,
            DatabaseName=database,
            TableName=table,
            Expression=f"region='{region}' and year={year} and month={month} and day={day} and hour={hour}"
        )
    
                
        if len(partitions['Partitions']) == 0:
            create_partition_response = glue_client.create_partition(
                CatalogId=catalog_id,
                DatabaseName=database,
                TableName=table,
                PartitionInput=input_dict
            )