import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# create glue 
logger.info(f"Boto3 Version : {boto3.__version__}")
glue_client = boto3.client('glue')

def get_databases():
    
    databases = []

    # paginated API call to fetch database names
    initial_response = glue_client.get_databases(
        AttributesToGet = [
            'NAME',
            ],
        ResourceShareType = 'ALL',
        MaxResults = 100
        )
        
    databases.extend(initial_response['DatabaseList'])

    while True:  
        if 'NextToken' in initial_response:
            logger.info(f'Next Token found in initial response, trigggering the GetDatabases API Call again')
            response = glue_client.get_databases(
                AttributesToGet = ['NAME'],
                ResourceShareType = 'ALL',
                MaxResults = 100,
                NextToken = initial_response['NextToken']
            )
            databases.extend(response['DatabaseList'])
            initial_response = response

        else:
            break

    # return list of database names which emd with curated
    curated_databases = [db['Name'] for db in databases if db['Name'].endswith('curated')]
    logger.info(f'Found curated databases: {curated_databases}')
    return curated_databases


def get_iceberg_tables(curated_database_name):
    tables = []

    # paginated API call to fetch table names
    initial_response = glue_client.get_tables(
        DatabaseName = curated_database_name,
        MaxResults = 100
        )

    tables.extend(initial_response['TableList'])
    
    while True:
        if 'NextToken' in initial_response:
            response = glue_client.get_tables(
                DatabaseName = curated_database_name,
                NextToken = initial_response['NextToken']
                )
            tables.extend(response['TableList'])
            initial_response = response
        else:
            break

    # return list of table names and database name as dictonary
    iceberg_tables = [table['Name'] for table in tables]
    if (len(iceberg_tables) > 0):
        logger.info(f'Found iceberg tables: {iceberg_tables}')
        return {'database_name':curated_database_name, 'tables':iceberg_tables}
    

def get_optimization_status(iceberg_table_list, account_id):
    
    tables_without_optimization = []
    database_tables_without_optimization = []
    
    for database in iceberg_table_list:
        batch_table_entries = []
        for table in database['tables']:
            batch_table_entries = [
                    {
                        'catalogId': account_id,
                        'databaseName': database['database_name'],
                        'tableName': table,
                        'type': 'compaction'
                    },
                    {
                        'catalogId': account_id,
                        'databaseName': database['database_name'],
                        'tableName': table,
                        'type': 'retention'
                    },
                    {
                        'catalogId': account_id,
                        'databaseName': database['database_name'],
                        'tableName': table,
                        'type': 'orphan_file_deletion'
                    }
                ]
            
            logger.info(f"Fetching Optimizer Details for {table}  in {database['database_name']}")
            response = glue_client.batch_get_table_optimizer(
                Entries = batch_table_entries
            )

            #logger.info(f"Get Table Optimizer Response : {response}")
            # parse through response and add find for which tables optimizers are not enabled
            
            if len(response['TableOptimizers']) == 0:
                database_tables_without_optimization.append(table)
            
        logger.info(f'''For Database {database['database_name']}, 
                Tables without optimization enabled: {database_tables_without_optimization}''')
        
        tables_without_optimization.append({
                'database_name':database['database_name'], 
                'tables':database_tables_without_optimization}
                )
    
    return tables_without_optimization

def enable_optimization(tables_without_optimization, account_id):
    for database in tables_without_optimization:
        for table in database['tables']:

            try:
                logger.info(f"Enabling Optimizer for table {table} in database {database['database_name']}")

                compaction_response = glue_client.create_table_optimizer(
                        CatalogId = account_id,
                        DatabaseName = database['database_name'],
                        TableName = table,
                        Type = 'compaction',
                        TableOptimizerConfiguration = {
                            'roleArn': os.environ['OPTIMIZER_ROLE_ARN'],
                            'enabled': True,
                            'vpcConfiguration': {
                                'glueConnectionName': os.environ['GLUE_CONNECTION_NAME']
                                }
                            }
                    )
                logger.info(f"Compaction Optimizer Enabled for table {table} in database {database['database_name']}")

                retention_response = glue_client.create_table_optimizer(
                    CatalogId = account_id,
                    DatabaseName = database['database_name'],
                    TableName = table,
                    Type = 'retention',
                    TableOptimizerConfiguration = {
                        'roleArn': os.environ['OPTIMIZER_ROLE_ARN'],
                        'enabled': True,
                        'vpcConfiguration': {
                            'glueConnectionName': os.environ['GLUE_CONNECTION_NAME']
                            },
                        'retentionConfiguration': {
                            'icebergConfiguration': {
                                'snapshotRetentionPeriodInDays': 5,
                                'numberOfSnapshotsToRetain': 2,
                                'cleanExpiredFiles': True
                            }
                        }
                    }
                )
                logger.info(f"Retention Optimizer Enabled for table {table} in database {database['database_name']}")

                orphan_file_response = glue_client.create_table_optimizer(
                    CatalogId = account_id,
                    DatabaseName = database['database_name'],
                    TableName = table,
                    Type = 'orphan_file_deletion',
                    TableOptimizerConfiguration = {
                        'roleArn': os.environ['OPTIMIZER_ROLE_ARN'],
                        'enabled': True,
                        'vpcConfiguration': {
                            'glueConnectionName': os.environ['GLUE_CONNECTION_NAME']
                            },
                        'orphanFileDeletionConfiguration': {
                            'icebergConfiguration': {
                                'orphanFileRetentionPeriodInDays': 3
                                }
                            }
                    }
                )
                logger.info(f"Orphan File Optimizer Enabled for table {table} in database {database['database_name']}")

            except Exception as e:
                logger.warning(f"Failed to enable Optimizer for table {table} in database {database['database_name']}")
                logger.warning(f"Failure Reason : {e}")

def lambda_handler(event, context):

    logger.info('Lambda Function function execution is starting')

    try:
        account_id = context.invoked_function_arn.split(":")[4]
        # get list of curated databases
        curated_databases = get_databases()
        iceberg_table_list = []

        # get list of iceberg tables for each curated database
        for database in curated_databases:
            table_list = get_iceberg_tables(database)
            if(table_list):
                iceberg_table_list.append(table_list)

        tables_without_optimization = get_optimization_status(iceberg_table_list,account_id)
        logger.info(f"Tables without optimization enabled: {tables_without_optimization}")

        enable_optimization(tables_without_optimization, account_id)
    
    except Exception as e:
        logger.error(f"Exception Occured : {e}")
        raise e