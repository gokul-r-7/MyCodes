import json
import boto3
import pandas as pd
import mysql.connector
from mysql.connector import errorcode
from secret_manager import username, password, hostname, databasename


s3 = boto3.client("s3")

def mysqldb_connection():
        mysqlconnection = None
        try:
            mysqlconnection = mysql.connector.connect(user=username, password=password, host=hostname, database=databasename)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        return mysqlconnection

def insert_data(insert_query, column_names):
    mysql_connection =  mysqldb_connection()
    cursor = mysql_connection.cursor()
    cursor.execute(insert_query, column_names)
    mysql_connection.commit()
    return "All the rows inserted"
    


def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    csv_file = event['Records'][0]['s3']['object']['key']
    
    separator = "."
    filename = csv_file.split(separator, 1)[0]
    
    csv_response = s3.get_object(Bucket= bucket_name, Key = csv_file)
    
    status = csv_response.get("ResponseMetadata", {}).get("HTTPStatusCode")

    if status == 200:
        print(f"Successful S3 get_object response. Status - {status}")
        
        csv_df = pd.read_csv(csv_response.get("Body"))

        csv_countofdf = csv_df.count
        csv_columns = list(csv_df.columns.values)
        column_length = len(csv_columns)
        sourcedata_filename = [filename] * column_length

        column_datatypes = []
        for i in csv_df.dtypes:
            column_datatypes.append(str(i))
            
        metadict = {'SourceFileName' : sourcedata_filename, 'ColumnNames' : csv_columns, 'DataTypes' :column_datatypes}
        
        metadatadf = pd.DataFrame(metadict)
        print(metadatadf)
        
        insert_query = "INSERT INTO MetaData (SourceFileName, ColumnNames, DataTypes) VALUES (%s, %s, %s)"

        for row in metadatadf.itertuples():
            column_names = (row.SourceFileName, row.ColumnNames, row.DataTypes)
            insert_data(insert_query, column_names)


    else:
        print(f"Unsuccessful S3 get_object response. Status - {status}")

    
    
    

    
    
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
