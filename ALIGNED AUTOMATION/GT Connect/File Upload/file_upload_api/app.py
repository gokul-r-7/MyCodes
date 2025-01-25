"""This module provides function for json"""
import json
import base64
import boto3
from botocore.exceptions import ClientError
import psycopg2
secret_manager = boto3.client("secretsmanager")
def get_secrets(secret_name):
    """Fetching data from AWS Secret Manager"""
    try:
        get_secret_value_response = secret_manager.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as secret_error:
        raise secret_error
    else:
        # Decrypts secret using the associated KMS key.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return decoded_binary_secret
def postgres_connection():
    """This function provides connection in oracle database"""
    secret_name = "RdsSecretforGtConnectAppUser"
    secret_manager_data = get_secrets(secret_name)
    rdssecrets= json.loads(secret_manager_data)
    username = rdssecrets['RDS_USERNAME']
    password = rdssecrets['RDS_PASSWORD']
    hostname = rdssecrets['RDS_HOST']
    postgresdbname = rdssecrets['RDS_DBNAME']
    portnumber = rdssecrets['RDS_PORT']
#    postgres_connection = None
    try:
        postgresdb_connection = psycopg2.connect(user = username, password = password, \
        host = hostname, port = portnumber, database = postgresdbname)
    except psycopg2.Error as postgres_error:
        return postgres_error
    return postgresdb_connection
def get_data(query):
    """This Function provides fetching data from oracle database"""
    postgresconnection = postgres_connection()
    cursor = postgresconnection.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    results = []
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results
def lambda_handler(event, context):
    """This lambda function provides oracle data as json format"""
    print(event, context)
    if event['resource'] == '/fileupload_api' \
    and event['path'] == '/fileupload_api' \
    and event['httpMethod'] == 'GET':
        if event['queryStringParameters'] is None:
            select_query = 'select * from gtconnect.source_transaction_data'
        else:
            arr = []
            query = 'select * from gtconnect.source_transaction_data where '
            for (key, value) in event['queryStringParameters'].items():
                arr.append(key + ' = '+  value + ' and ')
            newquery = ''
            newquery = ''.join(arr)
            newquery=  newquery[:-4]
            select_query = query + newquery
        results = get_data(select_query)
        response_object = {}
        response_object['statusCode'] = 200
        response_object['headers'] = {}
        response_object['headers']['Content-Type']='application/json'
        response_object['body'] = json.dumps(results, default=str)
    else:
        response_object = {}
        response_object['statusCode'] = 501
        response_object['headers'] = {}
        response_object['headers']['Content-Type'] = 'application/json'
        results = "The requested resource is not found"
        response_object['body'] = json.dumps(results, default=str)
    return response_object
