import json
import pandas as pd
from pandas import json_normalize
from PostgresConnection import *

def insert_data(insert_query, column_names):
    oracle_connection =  oracledb_connection()
    cursor = oracle_connection.cursor()
    cursor.execute(insert_query, column_names)
    oracle_connection.commit()
    return "All the rows inserted"

def lambda_handler(event, context):

    
    df = json_normalize(event)
    print(df)
    
    insert_query = "INSERT INTO BOOKSNEW values(:id, :published, :author, :title, :first_sentence, :language)"

    for row in df.itertuples():
        column_names = (row.ID, row.PUBLISHED, row.AUTHOR, row.TITLE, row.FIRST_SENTENCE, row.LANGUAGE)
        insert_data(insert_query, column_names)
        

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
