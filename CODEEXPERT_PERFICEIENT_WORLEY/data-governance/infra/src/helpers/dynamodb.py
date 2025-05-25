from .utilities import utilities
from boto3.dynamodb.conditions import Key

utils = utilities()
session = utils.get_aws_session()
logger = utils.get_logger()
settings = utils.get_settings()

class dynamodb:
    dynamodb_client = session.client('dynamodb')
    dynamodb_resource = session.resource('dynamodb')

    def __init__(self) -> None:
        pass

    def get_all_db_items(self,table_name):
        logger.info(f"Getting all items from {table_name}")
        table = self.dynamodb_resource.Table(table_name)
        items = table.scan()
        response = items['Items']
        while 'LastEvaluatedKey' in items:
            items = table.scan(ExclusiveStartKey=items['LastEvaluatedKey'])
            response.extend(items['Items'])
        return response

    def scan_by_domain(self, table_name, domain_name):
        logger.info(f"Scanning {table_name} by domain {domain_name}")
        table = self.dynamodb_resource.Table(table_name)
        response = table.scan(
            FilterExpression=Key('domain_name').eq(domain_name.lower())
        )
        return response['Items']

    def put_settings(self, table_name):
        tag_config = utils.get_tag_config()
        tag_keys = list(tag_config.keys())

        table = self.dynamodb_resource.Table(table_name)
        for i in tag_keys:
            item = {
                'tag_name': i,
                'tag_settings': tag_config[i]
            }
            logger.info(f"Putting item {item} into {table_name}")
            table.put_item(Item=item)

    def query_table(self, table_name, partition_key, partition_value, sort_key=None, sort_value=None):
        query_params = {
            'TableName': table_name,
            'KeyConditionExpression': f'{partition_key} = :partition_val',
            'ExpressionAttributeValues': {
                ':partition_val': {'S': partition_value}
            }
        }

        if sort_key and sort_value:
            query_params['KeyConditionExpression'] += f' AND {sort_key} = :sort_val'
            query_params['ExpressionAttributeValues'][':sort_val'] = {'S': sort_value}

        try:
            response = self.dynamodb_client.query(**query_params)
            return response['Items']
        except Exception as e:
            print(f"Error querying DynamoDB: {str(e)}")
            return None
        
    def get_database_names_by_domain(self,domain):
        # Get a reference to the DynamoDB table
        table_name = settings["database_permissions_store"]
        table = self.dynamodb_resource.Table(table_name)
    
        # Scan the table and filter by domain
        response = table.scan(
            FilterExpression=Key('domain_name').eq(domain),
            ProjectionExpression='database_name'
        )
    
        # Extract the database_name values from the response
        database_names = [item['database_name'] for item in response['Items']]
    
        # Handle potential pagination of results
        while 'LastEvaluatedKey' in response:
            response = table.scan(
                FilterExpression=Key('domain_name').eq(domain),
                ProjectionExpression='database_name',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            database_names.extend([item['database_name'] for item in response['Items']])
    
        # Filter database_names to include only those containing the keyword "curated"
        curated_database_names = [name for name in database_names if "curated" in name.lower()]
    
        return curated_database_names

    def get_source_name_by_database(self,database_name):
        table_name = settings["database_permissions_store"]
        table = self.dynamodb_resource.Table(table_name)
    
        # Query the table to get the item with the specified database_name
        response = table.query(
            KeyConditionExpression=Key('database_name').eq(database_name),
            ProjectionExpression='source_name'
        )
        
        return response['Items'][0]['source_name']


if __name__ == "__main__":
    dynamo = dynamodb()
    # res = dynamo.get_all_db_items('temp-database-permissions-metadata')
    res = dynamo.put_settings('worley-mf-domain-role-permissions-metadata')
    results = dynamo.query_table('temp-database-permissions-metadata', 'database_name', 'worley_datalake_sydney_dev_glue_catalog_database_construction_o3_raw')
    print(results)
