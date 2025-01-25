"""
    Use: This program transfers the data for GSIP from S3 Raw to Refine Aurora DB
    Author: Sumit Singh
    Updated On: 09-06-2021
    Pylint E0401 comes up when is not able to
    recognize external libraries like boto3 and pyspark functions.
    Pylint C0412 comes when ungrouped-imports are recognized
    Pylint W0401 comes up when wild-card import is recognized
    Pylint E0611 comes up when no-name-in-module is recognized
    Pylint W0703 comes up when broad-except is recognized"""
# pylint: disable = E0401, C0412, W0401, W0703, E0611
import sys
import io
import os
import datetime
import time
import json
import traceback
import boto3
import botocore
import pandas
import pg8000
import pyspark
import shutil
from awsglue.utils import getResolvedOptions
from pyspark.sql.utils import AnalysisException
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as f
from pyspark.sql.functions import concat, col, lit
from pyspark.sql.functions import *
from pyspark.context import *
from edb_glue_libraries.common.utils import get_db_secret_mgr
from pyspark.sql.types import DecimalType
from pyspark.sql.types import StructType
from awsglue.dynamicframe import DynamicFrame



SC = SparkContext()
GC = GlueContext(SC)
SPARK = GC.spark_session
JOB = Job(GC)

def init_config(config_common):
    """
    Initialize config and glue job
    """
    try:
        # Read Glue Parameters
        args = getResolvedOptions(sys.argv,
                                  ['JOB_NAME',
                                   'batch_name',
                                   'job_name',
                                   'region_name',
                                   'param_name',
                                   'common_param_name',
                                   'batch_execution_id',
                                   'process_execution_id',
                                   'abc_fetch_param_function',
                                   'abc_log_stats_func',
                                   'snow_func'])
        print("process execution id = ", args["process_execution_id"])
        JOB.init(args['JOB_NAME'], args)
        json_payload = {
            "batch_name": args.get("batch_name"),
            "job_name": args.get("job_name")}
        config_dct = invoke_lambda(args['abc_fetch_param_function'], json_payload)
        #print("Config dict from ABC :", config_dct)
        config = config_dct.get(args['param_name'])
        config_common = config[args['common_param_name']]
        config_common["job_name"] = args['JOB_NAME']
        config_common["process_execution_id"] = args['process_execution_id']
        config_common["snow_func"] = args['snow_func']
        config_common["abc_log_stats_func"] = args['abc_log_stats_func']
        config_common["batch_execution_id"] = args['batch_execution_id']
        #print("Config common:", config_common)
        return config_common, config
    except Exception as exep_func:
        print("Error in initialising config", str(exep_func))
        raise exep_func

def invoke_lambda(
        func_name,
        payload,
        region_name='us-east-2',
        invocation_type='RequestResponse'):
    """
    Invoke ABC 2.0 Lambda synchronously
    """
    lambda_client = boto3.client('lambda', region_name=region_name)
    if invocation_type == 'RequestResponse':
        invoke_response = lambda_client.invoke(
            FunctionName=func_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload))
        config_obj = json.loads(invoke_response['Payload'].read())
    else:
        lambda_client.invoke(
            FunctionName=func_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload))
        config_obj = {}
    return config_obj

def connect_db(config_object):
    """
    Method to connect Aurora db using pg8000
    """
    try:

        conn_dict = {'username': config_object['username'],
                     'password': config_object['password'],
                     'host': config_object['host'],
                     'port': int(config_object['port']),
                     'dbname': config_object['dbname']}
        conn = pg8000.connect(
            database=conn_dict.get("dbname"),
            user=conn_dict.get("username"),
            password=conn_dict.get("password"),
            host=conn_dict.get("host"),
            port=conn_dict.get("port"),
            ssl=True)
        return conn
    except Exception as exep_func:
        print('Error in db connection method', str(exep_func))
        raise exep_func

def transform_columns(table_name,config_child):

    """
    Method to transform columns.
    """

    User = config_child['user']
    loadAftTrim = config_child['LoadAftrTrimTx']
    loadIfNum = config_child['LoadIfNumTx']
    loadIfBool = config_child['LoadIfBoolTx']
    loadIfDate = config_child['LoadIfDateTx']
    loadIfDecimal = config_child['LoadIfDecimalTx']
    curDate = config_child['CurrentDate']
    eventHub = config_child['UserRun']
    srcSys = config_child['SrcSystem']
    batchExeId = config_child['batchExecutionId']
    proExeId = config_child['processExecutionId']
    concatId = config_child['concateId']
    recStatus = config_child['recordStatus']
    hubSrc = table_name.split('_')[3:len(table_name)]
    hubSrcSys = 'MERCURY' + ' ' + ' '.join(hubSrc)

    try :
        try :
            #create DynamicFame from glue catalog 
            src_df = GC.create_dynamic_frame.from_catalog(config_child['crawlerdb'], \
                                                                    config_child['crawler_tblName'], \
                                                                    redshift_tmp_dir="", \
                                                                    additional_options= \
                                                                    {"mergeSchema": "true"}, \
                                                                    transformation_ctx="src_df")
        except AnalysisException:
            sql_context = SQLContext(SC)
            empty_df = sql_context.createDataFrame(SC.emptyRDD(), StructType([]))
            src_df = DynamicFrame(empty_df, GC)
        
        if src_df.count() > 0:
            config_child['src_count'] = src_df.count()            
            #Converting glue dynamic frame to spark dataframe
            tgt_df = src_df.toDF()
            #Applying Straight load after trimming transformation
            for a1 in loadAftTrim:
                tgt_df = tgt_df.withColumn(a1,trim(col(a1)))  
            #Applying Straight load if valid date format, else null transformation
            for a2 in loadIfDate:
                #r1 = "([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})"
                r1 = "([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}+[0-9]{2})"
                r2 = "([0-9]{4}-[0-9]{2}-[0-9]{2})"
                tgt_df = tgt_df.withColumn(a2 + '_refined', when(f.col(a2).rlike(r1) == "true",(substring(col(a2), 0, 19)).cast("timestamp")).when(f.col(a2).rlike(r2) == "true",(substring(col(a2), 0, 10)).cast("timestamp")).otherwise(None)).withColumn(a2+'_raw',col(a2)).drop(a2)
            #Applying Straight load if valid number, else null transformation
            for a3 in loadIfNum:
                tgt_df = tgt_df.withColumn(a3 + '_refined',when(f.col(a3).rlike("^[0-9]*$") == 'true',col(a3).cast('int')).otherwise(None)).withColumn(a3+'_raw',col(a3)).drop(a3)
            #Applying If true - 1 , if false - 0, else null transformation
            for a4 in loadIfBool:
                tgt_df = tgt_df.withColumn(a4 + '_refined',when(f.col(a4) == 'true',1).when(f.col(a4) == 'false',0).otherwise(None)).withColumn(a4+'_raw',col(a4)).drop(a4)
            #Applying Straight load if valid decimal field, else null transformation
            for a5 in loadIfDecimal:
                tgt_df = tgt_df.withColumn(a5 + '_refined', when(f.col(a5).rlike("^\d*[.]\d*$") == 'true',col(a5).cast(DecimalType())).otherwise(None)).withColumn(a5+ '_raw',col(a5)).drop(a5)
            #Applying current date transformation
            for a6 in curDate:
                tgt_df = tgt_df.withColumn(a6,lit(current_timestamp()))
            for a7 in eventHub:
                tgt_df = tgt_df.withColumn(a7,lit(User))
            for a8 in srcSys:
                tgt_df = tgt_df.withColumn(a8,lit(hubSrcSys))
            tgt_df = tgt_df.withColumn(batchExeId,lit(config_child["batch_execution_id"])).withColumn(proExeId,lit(config_child["process_execution_id"]))
            tgt_df = tgt_df.withColumn(batchExeId,col(batchExeId).cast('int')).withColumn(proExeId,col(proExeId).cast('int'))
            tgt_df =tgt_df.withColumn(concatId, concat(col('id'),lit('|'),col(batchExeId))).withColumn(recStatus,lit('Y')).drop('partition_0')
            if 'account' in config_child['crawler_tblName']:
                tgt_df = tgt_df.withColumn('billingaddress',lit(' '))
                tgt_df = tgt_df.withColumn('shippingaddress',lit(' '))
                tgt_df = tgt_df.withColumn('personmailingaddress',lit(' '))
                tgt_df = tgt_df.withColumn('personotheraddress',lit(' '))
            
            tgt_df = tgt_df.sort(col("id").asc(),col("lastmodifieddate_refined").desc()).dropDuplicates(["id"])
            return tgt_df
        else :
            print('No files in input!!')
            return src_df
    except Exception as exep_func:
            print('Error in applying transformations', str(exep_func))
            raise exep_func

def copy_s3_files(config_object):
    """
    Function to copy S3 files from one location to another
    """
    try:        
        old_bucket_name = config_object["src_bucket"]
        old_prefix = config_object["old_prefix"]
        new_bucket_name = config_object["tgt_bucket"]
        new_prefix = config_object["new_prefix"] + datetime.datetime.now().strftime("%m-%d-%Y %H:%M:%S") + '/'
        s3 = boto3.resource('s3')
        old_bucket = s3.Bucket(old_bucket_name)
        new_bucket = s3.Bucket(new_bucket_name)
        for obj in old_bucket.objects.filter(Prefix=old_prefix):
            old_source = { 'Bucket': old_bucket_name,
						   'Key': obj.key}
			# replace the prefix
            new_key = obj.key.replace(old_prefix, new_prefix, 1)
            new_obj = new_bucket.Object(new_key)
            new_obj.copy(old_source)
    except Exception as exp_handler:
        print("Error in archiving s3 files", str(exp_handler))
        raise exp_handler
def delete_s3_files(config_object):
    """
    Function to delete files from S3
    """
    try:
        old_prefix = config_object["old_prefix"]
        s3 = boto3.resource('s3')
        src_bucket_name = s3.Bucket(config_object["src_bucket"])
        for obj in src_bucket_name.objects.filter(Prefix=old_prefix):
            s3.Object(src_bucket_name.name,obj.key).delete()
        
    except Exception as exp_handler:
        print("Error in deleting s3 files", str(exp_handler))
        raise exp_handler

def get_affected_rows_count(conn, inp_payload, stmt_type):
    """
    Function to get affected count from Aurora DB
    """
    try:
        if stmt_type == 'TYPE2_UPDATE':
            cursor = conn.cursor()
            update_query = inp_payload['tgt_update_count_query'].replace('<schema_name>', inp_payload['schema_name']) 
            cursor.execute(update_query)
            count = cursor.fetchall()
            _count = count[0][0]
            conn.commit()
            return _count
        else:
            cursor = conn.cursor()
            insert_query = inp_payload['tgt_insert_count_query'].replace('<schema_name>', inp_payload['schema_name']) 
            cursor.execute(insert_query)
            count = cursor.fetchall()
            _count = count[0][0]
            conn.commit()
            return _count
    except Exception as exp:
        print(f"Job failed with some exception {traceback.format_exc()}")
        print(exp)
        raise

def rollback_database_table_process(schema, tb_name, process_id, cursor, conn):
    """
    Rollback from Refined Db if load is unsuccessful
    """
    try:
        print(process_id)
        delete_query = f"DELETE FROM {schema}.{tb_name} WHERE edb_gsip_merc_process_exec_id = {process_id}"
        cursor.execute(delete_query)
        conn.commit()
        print(f"Table Deleted {schema}.{tb_name}")
        conn.commit()
    except Exception as ex:
        print(ex)
        print(f"Rollback failed due to Error {traceback.format_exc()}")
        raise ex

def get_success_payload_temp(inp_payload, tmp_ins_count,tblName):
    """
    Function to invoke ABC success payload
    """
    json_payload = {
        "process_execution_id": inp_payload['process_execution_id'],
        "component_type_code": "ETL",
        "component_name": \
        "Data load from S3 to Refine Aurora DB",
        "logical_source_name": "Raw S3 " + tblName,
        "logical_target_name": "Refine Aurora Temp table for " + inp_payload['temp_table_name'],
        "records_read_count": tmp_ins_count,
        "records_inserted_count": inp_payload['ins_count'],
        "records_updated_count": 0,
        "records_upserted_count": 0,
        "records_deleted_count": 0,
        "records_rejected_count": 0,
        "records_no_change_count": 0,
        "etl_operation_source_object_type_name": "S3",
        "etl_operation_target_object_type_name": "DB_TABLE",
        "etl_operation_source_object_table_name": tblName,
        "etl_operation_target_object_table_name": inp_payload['temp_table_name'],
        "etl_operation_source_object_schema_name": inp_payload['crawlerdb'],
        "etl_operation_target_object_schema_name": inp_payload['schema_name'],
        }
    print("Json Payload temp", json_payload)
    return json_payload


def get_success_payload_target(inp_payload):
    """
    Function to invoke ABC success payload
    """
    json_payload = {
        "process_execution_id": inp_payload['process_execution_id'],
        "component_type_code": "ETL",
        "component_name": \
        "Data load from Temp to Target table in Refine Aurora DB",
        "logical_source_name": "Refine Aurora Temp Table for " + \
                                str(inp_payload['temp_table_name']),
        "logical_target_name": "Refine Aurora Target table for " + inp_payload['target_table_name'],
        "records_read_count": inp_payload['ins_count'],
        "records_inserted_count": inp_payload['tgt_insert_count'],
        "records_updated_count": inp_payload['tgt_updt_count'],
        "records_upserted_count": inp_payload['tgt_upsert_count'],
        "records_deleted_count": 0,
        "records_rejected_count": 0,
        "records_no_change_count": 0,
        "etl_operation_source_object_type_name": "DB_TABLE",
        "etl_operation_target_object_type_name": "DB_TABLE",
        "etl_operation_source_object_table_name": inp_payload['temp_table_name'],
        "etl_operation_target_object_table_name": inp_payload['target_table_name'],
        "etl_operation_source_object_schema_name": inp_payload['schema_name'],
        "etl_operation_target_object_schema_name": inp_payload['schema_name'],
        }
    print("Json Payload temp", json_payload)
    return json_payload

def load_temp_target_tables(table, config_child, temp_df, conn):
    """
    Method to load data in temp and target tables
    """
    cursor = None
    try:
        cursor = conn.cursor()
        set_work_mem = "SET work_mem = '50MB'"
        cursor.execute(set_work_mem)
        temp_tbl_name = config_child["temp_table_name"]
        df_count = temp_df.count()

        # Insert into temp  
        try:
            errorFlag = False
            temp_table_nm = config_child['schema_name'] + '.' + temp_tbl_name
            temp_df = temp_df.coalesce(4)
            url = "jdbc:postgresql://" + config_child['host'] + ":" + str(config_child['port']) \
                                + "/" + config_child['dbname'] + "?sslmode=require&ssl=true"
            print('url', url)
            print('user', config_child['username'])
            print('dbtable', temp_table_nm)

            temp_df.write.format("jdbc").options(url=url, \
                                                user=config_child['username'],\
                                                dbtable=temp_table_nm,\
                                                password=config_child['password'],\
                                                truncate=True) \
                                                .mode('Overwrite').save()
            print('here1')                                    
            insert_count = "SELECT COUNT(1) FROM {}.{}".format(config_child['schema_name'],temp_tbl_name)
            cursor.execute(insert_count)                                    
            config_child["ins_count"] = cursor.rowcount
            conn.commit()

            # ABC Process Execution Stats
            json_payload = get_success_payload_temp(config_child, df_count,config_child['crawler_tblName'])
            invoke_lambda(config_child["abc_log_stats_func"], json_payload, 'us-east-2', 'Event')
            print("ABC 2.0 process execution stats generated for temp tables", json_payload)
        
        except Exception as exep_func:
            errorFlag = True
            print('Error in write db table method', str(exep_func))
            print("Error in loading target table : {}".format(temp_tbl_name))
            rollback_database_table_process(config_child['schema_name'], temp_tbl_name,
                                        config_child['process_execution_id'], cursor, conn)
            raise exep_func

        if errorFlag == False:
            print('errorFlag is false')
            try:
                #Upsert into Target using common utils
                tgt_tbl_name = config_child["target_table_name"]
                print('target_table_name', tgt_tbl_name)
                upsert_query = config_child['target_upsert_query'].replace('<schema_name>', config_child['schema_name']) 
                print('upsert_query', upsert_query)
                cursor.execute(upsert_query)
                config_child["ups_count"] = cursor.rowcount
                conn.commit()
                print('upsert count ', config_child["ups_count"])
                #Get target insert and update count
                #get target counts
                config_child['tgt_updt_count'] = 0
                config_child['tgt_insert_count'] = 0
                config_child['tgt_upsert_count'] = 0
                config_child['tgt_updt_count'] = get_affected_rows_count(conn, config_child, 'TYPE2_UPDATE')
                config_child['tgt_insert_count'] = get_affected_rows_count(conn, config_child, 'TYPE2_INSERT')
                print('target insert ', config_child['tgt_insert_count'])
                print('target update ', config_child['tgt_updt_count'])

                # ABC Process Execution Stats
                json_payload = get_success_payload_target(config_child)
                invoke_lambda(config_child["abc_log_stats_func"], json_payload, 'us-east-2', 'Event')
                print("ABC 2.0 process execution stats generated for target tables")
                return config_child
            except Exception as ex:
                print(ex)
                print("Error in loading target table : {}".format(tgt_tbl_name))
                rollback_database_table_process(config_child['schema_name'], tgt_tbl_name,
                                                config_child['process_execution_id'], cursor, conn)
                config_child['error_message'] = 'Error while writing to temp and target tables'
                config_child['error_type'] = str(ex)
                raise ex
        else:
            print("Target table not loaded")
    finally:
        if cursor != None:
            cursor.close()

def main():
    """
    This function transfers the data from S3 to refined Aurora db with SCD2
    """
    try:
        config_common = {}
        config_common, config = init_config(config_common)
        #table_list = config_common["table_list"]
        # Get database credentials from Secret Manager
        db_conn_dict = get_db_secret_mgr(config_common["secret_id"], \
        config_common["region_name"])
        config_common.update(db_conn_dict)
        #print('config_common for db:', config_common)
        db_conn = connect_db(config_common)
        #for table in table_list:
        table = config_common['table_name']
        #config_common['table_name'] = table
        print('Table is: ', table)
        child_grp_name = table
        config_child = config.get(child_grp_name)
        # This dict has common db and table details
        config_child.update(config_common)
        #print('Config child is for the given table: ', config_child)
        temp_df = transform_columns(table,config_child)
        if temp_df.count() > 0:
            configchild = load_temp_target_tables(table, config_child, temp_df, db_conn)
            print("Data Load successful for :",table)
            copy_s3_files(config_child)
            delete_s3_files(config_child)
    except Exception as exp:
        print("Error in main function", str(exp))
        raise
    finally:
        print("inside finally block")
        db_conn.close()

if __name__ == "__main__":
    main()
    
