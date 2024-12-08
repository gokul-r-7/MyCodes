import json
from OracleConnection import *
#from flask_lambda import FlaskLambda
#from flask import request

#app = FlaskLambda(__name__)

def get_data(query):
    oracle_connection = oracledb_connection()
    cursor = oracle_connection.cursor()
    cursor.execute(query)
    columns = [col[0] for col in cursor.description]
    cursor.rowfactory = lambda *args: dict(zip(columns, args))
    json_data = cursor.fetchall()
    return json_data

# import requests

#@app.route('/searchtransaction', method =['GET'])
def lambda_handler(event, context):

    if event['resource'] == '/tva-search' and event['httpMethod'] == 'GET' and event['path'] == '/tva-search':
        select_query = "Select * from books" 
        results = get_data(select_query)
        
        responseObject = {}
        responseObject['statusCode'] = 200
        responseObject['headers'] = {}
        responseObject['headers']['Content-Type'] = 'application/json'
        responseObject['body'] = json.dumps(results)
    else:
        results = "The requested resource is not found" 
        responseObject = {}
        responseObject['statusCode'] = 501
        responseObject['headers'] = {}
        responseObject['headers']['Content-Type'] = 'application/json'
        responseObject['body'] = json.dumps(results)
        
    return responseObject