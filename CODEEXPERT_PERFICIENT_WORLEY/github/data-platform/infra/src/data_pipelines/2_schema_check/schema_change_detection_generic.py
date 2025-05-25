import boto3
import pandas as pd
import sys
import logging
import json


from datetime import datetime
from awsglue.utils import getResolvedOptions
from botocore.exceptions import ClientError

glue_client = boto3.client('glue')



MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)



# Function to compare the name and type of columns in new column list with old column list to 
# find any newly added column and the columns with changed data type
def findAddedUpdated(new_cols_df, old_cols_df, old_col_name_list):
        for index, row in new_cols_df.iterrows():
            new_col_name = new_cols_df.iloc[index]['Name']
            new_col_type = new_cols_df.iloc[index]['Type']

            # Check if a column with same name exist in old table but the data type has changed
            if new_col_name in old_col_name_list:
                old_col_idx = old_cols_df.index[old_cols_df['Name']==new_col_name][0]
                old_col_type = old_cols_df.iloc[old_col_idx]['Type']

                if old_col_type != new_col_type:
                    columns_modified.append(f"Data type changed for '{new_col_name}' from '{old_col_type}' to '{new_col_type}'")
            # If a column is only in new column list, it a newly added column
            else:
                columns_modified.append(f"Added new column '{new_col_name}' with data type as '{new_col_type}'")


# Function to iterate through the list of old columns and check if any column doesn't exist in new columns list to find out dropped columns
def findDropped(old_cols_df, new_col_name_list):
        for index, row in old_cols_df.iterrows():
            old_col_name = old_cols_df.iloc[index]['Name']
            old_col_type = old_cols_df.iloc[index]['Type']

            #check if column doesn't exist in new column list  
            if old_col_name not in new_col_name_list:
                columns_modified.append(f"Dropped old column '{old_col_name}' with data type as '{old_col_type}'")


# Function to convert version_id to int to use for sorting the versions
def version_id(json):
        try:
            return int(json['VersionId'])
        except KeyError:
            return 0
            

# Function to delete the table versions
def delete_versions(table_name,db_name,versions_list, number_of_versions_to_retain):
        if len(versions_list) > number_of_versions_to_retain:
            logger.info(f"Fetching the versions to be deleted")
            version_id_list = []
            for table_version in versions_list:
                version_id_list.append(int(table_version['VersionId']))

            versions_str_list = [str(x) for x in version_id_list]
            versions_to_delete = versions_str_list[number_of_versions_to_retain:]
            
            
            try:
                del_response = glue_client.batch_delete_table_version(
                    DatabaseName=db_name,
                    TableName=table_name,
                    VersionIds=versions_to_delete
                )
                logger.info(f"Completed deleting old versions of {db_name}.{table_name}")
                
            except ClientError as err:
                logger.error(f"Deleting Table Versions Failed. Error Code : {err.response['Error']['Code']}. Error Message : {err.response['Error']['Message']}")
                raise
                
            
def version_id(table_info):
        try:
            return int(table_info['VersionId'])
        except KeyError:
            return 0


# Retriving the Table versions in GLue Data Catalog
def get_glue_table_versions(db_name,table_name):
    
    try:
        table_version_response = glue_client.get_table_versions(
                     DatabaseName=db_name,
                      TableName=table_name,
                      MaxResults=15
                     )
                     
    except ClientError as err:
        logger.error(f"Fetching Glue Table Version Failed. Error Code : {err.response['Error']['Code']}. Error Mesage : {err.response['Error']['Message']}")
        raise
    
    table_versions_info = table_version_response['TableVersions']
    
    #sorting table version in descending order of table version
    table_versions_info.sort(key=version_id,reverse=True)
    
    logger.info(f"Glue Get Table Version Response API Call : {str(table_versions_info)}")
    return table_versions_info


# Function to publish changes in schema to a SNS topic that can be subscribed to receive email notifications when changes are detected
def notifyChanges(message_to_send):
    sns = boto3.client('sns')
    
    # Publish a message to the specified SNS Topic
    try:
        response = sns.publish(
                TopicArn= sns_topic_arn,   
                Message=message_to_send,  
                Subject="Data Platform Schema Change Notification: Changes in table schema"
            )
        logger.info(f"Completed Sending SNS Notification")
        
    except ClientError as err:
         logger.error(f"Publishing SNS Notification Failed. Error Code : {err.response['Error']['Code']}. Error Mesage : {err.response['Error']['Message']}")
         raise
        
        

logger.info(f'Starting Glue Job for Detecting Schema Changes within Glue Table')

input_args = getResolvedOptions(sys.argv,
                          [
                           'catalog_db',
                           'table_name',
                           'topic_arn',
                           'region_name',
                           'region_short_name'
                           ]
                        )

logger.info(f'Fetching relevant input arguments')


sns_topic_arn=input_args['topic_arn']
db_name=input_args['catalog_db']
table_name=input_args['table_name']

delete_old_versions = False
number_of_versions_to_retain = 5
versions_to_compare=[0,1]
columns_modified = []
sns_message = ''


try:
    table_versions = get_glue_table_versions(db_name,table_name)
    logger.info(f'Retreived Table Versions for Table : {db_name}.{table_name}')
    
    
    
    table_versions_ids = list(map(lambda x: int(x["VersionId"]), table_versions))
    logger.info(f"Table Version ID's : {table_versions_ids}")
    
    no_of_table_versions = len(table_versions_ids)
    
    # Skip Schema Change Detection when there is only 1 schema version
    if(no_of_table_versions < 2):
        logger.info(f"There is only one schema version of {db_name}.{table_name}. Hence, skipping schema change detection")
        logger.info(f"Ending Schema Change Detection Job for {db_name}.{table_name}")
        sys.exit(0)
    
        
    logger.info(f'Latest Version ID of Table {db_name}.{table_name} after running Glue Crawler : {table_versions_ids[0]}')
    logger.info(f'Previous Version ID of Table {db_name}.{table_name} before running Glue Crawler : {table_versions_ids[1]}')
    
    latest_version_columns = table_versions[0]['Table']['StorageDescriptor']['Columns']
    latest_version_column_df =  pd.DataFrame(latest_version_columns)
    logger.info(f'DataFrame creation for latest schema columns completed')
    
    
    previous_version_columns = table_versions[1]['Table']['StorageDescriptor']['Columns']
    previous_version_column_df =  pd.DataFrame(previous_version_columns)
    logger.info(f'DataFrame creation for previous schema columns completed')
    
    
    latest_version_column_list =  latest_version_column_df['Name'].tolist()
    previous_version_column_list =  previous_version_column_df['Name'].tolist()
    
    
    findAddedUpdated(latest_version_column_df, previous_version_column_df, previous_version_column_list)
    logger.info(f"Completed  identification of columns which are added/updated")
    
    findDropped(previous_version_column_df, latest_version_column_list)
    logger.info("Completed identification of columns which are dropped")
    
    if len(columns_modified) > 0:
        email_msg = f"Following changes are identified in '{table_name}' table of '{db_name}' database of your Data Platform. Please review.\n\n"
        logger.info(f"Schema Change Detection Job completed for '{table_name}' table of '{db_name}' ! -- below is list of changes.")
        for column_modified in columns_modified:
            email_msg += f"\t{column_modified}\n"
        
        logger.info(email_msg)
        notifyChanges(email_msg)
        sys.exit(email_msg)
    
    else:
        message = f"Schema Change Detection Job for '{table_name}' table of '{db_name}' -- There are no changes in table schema."
        logger.info(message)
    
    if delete_old_versions:
        delete_versions(table_name,db_name,table_versions,number_of_versions_to_retain)
        message = "Job completed! -- There are no changes in table schema."
        logger.info(message)

except Exception as err:
    failure_message = str(err)
    logger.error(f"Schema Change Detection Job Failed. Exception Details -  {str(err)}")
    sys.exit(failure_message)