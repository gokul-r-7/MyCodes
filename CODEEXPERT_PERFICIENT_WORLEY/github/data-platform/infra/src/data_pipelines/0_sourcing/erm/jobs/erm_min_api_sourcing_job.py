import sys
import re
import json
import base64
import boto3
import os
import time
import traceback
import io
import csv
from datetime import datetime, timezone
import datetime as dt
from pyspark.sql import Window
from pyspark.sql.functions import lit, col, concat_ws, udf, coalesce, collect_list, trim, regexp_replace, date_format, expr, row_number, to_date
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import get_partition_str_mi, write_glue_df_to_s3_with_specific_file_name
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import generate_timestamp_string, generate_today_date_string
from worley_helper.utils.constants import TIMEZONE_SYDNEY, DATETIME_FORMAT, REGION, DATE_FORMAT, AUDIT_DATE_COLUMN
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from worley_helper.utils.http_api_client import HTTPClient
from worley_helper.utils.email_helpers import send_email, generate_success_email_body, generate_failure_email_body
from pyspark.sql.functions import col, when
import xml.etree.ElementTree as ET
from awsglue.transforms import ApplyMapping
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, struct, array
import pandas as pd
import copy

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext

#sc = SparkSession.builder.appName("GlueJob").getOrCreate()
sc = SparkConf()
# sc = SparkContext()

# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv, ["JOB_NAME","source_name",
               "function_name", "metadata_table_name", "project_id", "bucket_name","environment", "drl_query_id", "picklist_query_id"]
)

logger.info(args)

source_name = args.get("source_name")
table_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
project_id_name = args["project_id"]
drl_query_id = args.get("drl_query_id", "Q1991")
picklist_query_id = args.get("picklist_query_id", "Q1989")
selected_voucher = args.get("voucherno", None)
bckt = args.get("bucket_name")
current_env = args.get("environment")
logger.info("ProjectId --> " + project_id_name)

sc.setAll([
    ('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions'),
    ('spark.sql.catalog.glue_catalog', 'org.apache.iceberg.spark.SparkCatalog'),
    ('spark.sql.catalog.glue_catalog.warehouse', f's3://{bckt}/supply_chain/erm-warehouse/'),
    ('spark.sql.catalog.glue_catalog.catalog-impl', 'org.apache.iceberg.aws.glue.GlueCatalog'),
    ('spark.sql.catalog.glue_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO'),
    ('spark.sql.defaultCatalog', 'glue_catalog'),
    ('spark.sql.autoBroadcastJoinThreshold', 268435456),
    # ('spark.sql.autoBroadcastJoinThreshold', -1)
])
spark_conf = SparkContext(conf=sc)
glueContext = GlueContext(spark_conf)
spark = glueContext.spark_session
job = Job(glueContext)
# Define the Sort Keys for DynamoDB Fetch
data_export_input_keys = "api#" + source_name + "#registerstatus"
function_key = "api#" + source_name + "#registerprocess"

# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=function_key
)

data_export_metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=data_export_input_keys
)

vouchers_processed = []
vouchers_failed = []


logger.info(f" Metadata Response :{metadata}")

erm_export_config = metadata
logger.info(f" ERM API Export Config :{erm_export_config}")

############## Load ERM Workflow Export API Config ################
bucket_name = erm_export_config['job_parameter']['bucket_name']
raw_data_location = erm_export_config['job_parameter']['input_path']
relationalized_data_path = erm_export_config['job_parameter']['temp_output_path']
relationalized_path = relationalized_data_path + \
    table_name + "/relationalized_data/" + table_name
parquet_data_path = erm_export_config['job_parameter']['output_s3']
region = erm_export_config['aws_region']
endpoint = erm_export_config['api_parameter']['endpoint']
sample_data_path = erm_export_config['job_parameter']['schema_output_s3']
sampling_fraction = float(
    erm_export_config['job_parameter']['sampling_fraction'])
sampling_seed = erm_export_config['job_parameter']['sampling_seed']
name = erm_export_config['name']
full_incr = erm_export_config['job_parameter']['full_incremental']
status_timeout = erm_export_config['job_parameter']['status_timeout']
status_check = erm_export_config['job_parameter']['status_check']
dependent_input_path = erm_export_config['job_parameter']['dependent_input_path']
is_depenednt_catalog = erm_export_config['job_parameter']['is_depenednt_from_redshift']
depenedent_data_local_full_load_file_name = erm_export_config['job_parameter']['depenedent_data_local_full_load_file_name']
depenedent_data_local_sample_file_name = erm_export_config['job_parameter']['depenedent_data_local_sample_file_name']
register_api_body = "PROJECT_ID;SITEMTOHDRID;PRODUCTIONDATE;MATERIALID;QUAN;UOM;SITEMTOLINEITEMID;STORE;FABCAT;LOCATION;VOUCHERNO;VOUCHERITEMNO;TRANSTYPE"
project_id_partition = f"project_id={project_id_name}"
kms_key_id = erm_export_config['job_parameter']['kms_key_id']
dependent_input_buckt = "worley-datalake-sydney-dev-bucket-curated-xd5ydg"
dependent_src_path = "supply_chain/erm/ril_min_number/data"
auth_type = erm_export_config['api_parameter']['auth_type']

sender_email = erm_export_config["sender_email"]
receiver_email = erm_export_config["receiver_email"]
receiver_email = receiver_email if isinstance(erm_export_config["receiver_email"], list) else list(receiver_email)
smtp_server = erm_export_config["smtp_server"]
smtp_port = erm_export_config["smtp_port"]

# Get the HTTP POST body parameters for the export API
s3_client = S3(bucket_name, region)
register_status_root_tag = "C Pext Audit Drl"
pick_status_root_tag = "C Pext Audit Pick"
# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(
    erm_export_config['auth_api_parameter']['secret_key'], region))

client_id = secret_param_key.get("client_id")
client_secret = secret_param_key.get("client_secret")

erm_export_config['auth_api_parameter']['auth_body']['client_id'] = client_id
erm_export_config['auth_api_parameter']['auth_body']['client_secret'] = client_secret

data_export_metadata['auth_api_parameter']['auth_body']['client_id'] = client_id
data_export_metadata['auth_api_parameter']['auth_body']['client_secret'] = client_secret

data_export_metadata['api_parameter']['api_method'] = 'get'


logger.info("Bucket Name -> " + bucket_name)
logger.info("raw_data_location -> " + raw_data_location)
logger.info("relationalized_data_path -> " + relationalized_data_path)
logger.info("parquet_data_path -> " + parquet_data_path)
logger.info("erm_export_config -> " + str(erm_export_config))
logger.info("full_incr --> " + full_incr)
logger.info("status_timeout --> " + str(status_timeout))
logger.info("status_check --> " + str(status_check))
folder_name="supply_chain/erm/erm_token"
file_name="erm_token.txt"
content,status=s3_client.read_s3_file(folder_name,file_name)
tf_ctxt = f"{source_name}_{table_name}_{project_id_name}_{selected_voucher}_2"
api_token = None
if content:
    api_token = content
headers = {}
headers['Authorization'] = f'Bearer {api_token}'
is_depenednt_catalog = True
concat_list = udf(lambda lst: '\n'.join(lst), StringType())

partition_date = generate_timestamp_string(
    timezone=TIMEZONE_SYDNEY
    ).strftime(DATETIME_FORMAT)
#logger.info(f"partition_date : {partition_date}")
partition_str = f"{get_partition_str_mi(partition_date)}"

# catalog: database and table names
erm_db_name = f"worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated"
tbl_ril_min = "ril_min_list"
tbl_draw = "draw"
tbl_site_mto_hdr = "site_mto_hdr"
tbl_site_mto_item = "site_mto_item"
tbl_mat = "mat"
tbl_deliv_desig = "deliv_desig"

min_data_clmn = ["issued_date","pts_code","final_qty","uom","min_number","drawing_number"]
draw_data_clmn = ["draw_name","draw_no","is_current"]
site_mto_hdr_data_clmn = ["draw_no","site_mto_hdr_no","site_mto_hdr_id","deliv_desig_no","is_current"]
smi_data_clmn = ["site_mto_hdr_no","line_no","mat_no","site_mto_item_no","is_current"]
mat_data_clmn = ["mat_no","mat_id","is_current"]
dd_data_clmn = ["deliv_desig_no","deliv_desig_id","is_current"]
requrired_api_data_columns = ["project_id","sitemtohdrid","productiondate","materialid","quan","uom","sitemtolineitemid","store","fabcat","location","voucherno","voucheritemno","transtype"]

def get_publish_status(workflow_publish_status_resp,api_status,status_code, root_tag):
    INPROGRESS = "INPROGRESS"
    logger.info(workflow_publish_status_resp)
    logger.info(api_status)
    logger.info(status_code)
    if not api_status or not workflow_publish_status_resp:
        logger.info(f"API failed for project_id")

    publish_status_resp_dict = workflow_publish_status_resp
    #json.load(publish_status_resp_json)
    publish_status = None
    if root_tag in publish_status_resp_dict:
        if len(publish_status_resp_dict[root_tag]) < 1:
            logger.info("EMPTY data")
            return "INPROGRESS" #As per David this could be 2 secnarios 1. Register API Payload is of wrong format or 2. Payload is correct and it is in-progress
        else:
            if not "STAT" in publish_status_resp_dict[root_tag][0]:
                logger.error(f"The Voucher Status Query response from API is not in expected format. So exiting with failure ")
                return "FAILED"
    else:
        logger.error(f"The Voucher Status Query response from API is not in expected format. So exiting with failure ")
        return "FAILED"
    
    return publish_status_resp_dict[root_tag][0]["STAT"]

def token_retry_mechanism(bucket_name, http_client):
    response, api_status, status_code = http_client.run()
    if status_code == 401:
        temp_path_deletion = "supply_chain/erm/erm_token/erm_token.txt"
        logger.info(f"Deleting folder path {temp_path_deletion}")
        s3_client.delete_file_from_s3(temp_path_deletion)
        response, api_status, status_code = http_client.run()
        logger.info(response)
        logger.info(api_status)
        logger.info(status_code)

    return response, api_status, status_code
    
def process_request_creation_status_updates(process_name, voucher_id, api_data_csv_strng, erm_export_config, data_export_config, bucket_name, query_id, status_root_tag, partition_str, s3_client):
    
    #logger.info(f"voucher - {voucher_data} and api data - {api_data_csv_strng}")
    data_collection_drl = []
    s = datetime.now()
    fmrt_tmt = datetime.strftime(s, '%Y%m%d%H%M%S')
    reg_interfaceid=f"{'DRL' if process_name == 'DRL' else 'PICK'}Interface"
    reg_filename=f"{'drl' if process_name == 'DRL' else 'PICK'}_min"

    drl_export_config = copy.deepcopy(erm_export_config)
    pick_export_config = copy.deepcopy(data_export_config)



    report_api_endpoint = drl_export_config['api_parameter']['endpoint']
    frmtd_endpoint = report_api_endpoint.format(reg_interfaceid=reg_interfaceid,reg_filename=reg_filename,current_timestamp=fmrt_tmt)
    drl_export_config['api_parameter']['endpoint'] = frmtd_endpoint
    
    #api_body_data = voucher_data["combined_param"]
    drl_export_config['api_parameter']['api_body'] = api_data_csv_strng
    # drl_export_config['api_parameter']['api_headers']['x-erm-env'] = 'production' if current_env == "prd" else "testing"  -- This value will come from dynamodb directly
    # drl_export_config['api_parameter']['api_headers']['Content-Type'] = 'text/csv'    -- This value will come from dynamodb directly

    logger.info(f"HTTP Request Info for creation request --> {drl_export_config['api_parameter']}")

    http_client = HTTPClient(drl_export_config)

    workflow_publish_id_response, api_status, status_code = token_retry_mechanism(bucket_name, http_client)

    logger.info(f" Validating PublishId Request to ERM using Response: {workflow_publish_id_response} , Status : {api_status}, Status Code : {status_code}")            
    if workflow_publish_id_response is None or status_code is None or not api_status:
        err_msg = f"Unable to generate PublishId from ERM APIs for vocucher No - {voucher_id}. Check if payload paramaters are correct. Error Response is {workflow_publish_id_response}"
        logger.info(f"{err_msg}")

        return False, err_msg

    # calling the API to check the status of API , either it can be Processing, Completed or CompletedwithErrors
    
    upload_key = workflow_publish_id_response.get("RegisterAndProcessImportResult")
    logger.info(f"Proceeding to check job status for PublishId {upload_key}")
    data_export_endpoint = pick_export_config['api_parameter']['endpoint']
    data_export_endpoint = data_export_endpoint.format(project_id=query_id)
    publish_param = f"'PrimaryKey':{upload_key}"
    #endpoint_get_publish_status = f"{data_export_endpoint}Query?id={project_id}&format=json&parameters="
    endpoint_get_publish_status = data_export_endpoint + "{" + publish_param + "}"
    pick_export_config['api_parameter']['endpoint'] = endpoint_get_publish_status
    # pick_export_config['api_parameter']['api_headers']['x-erm-env'] = 'production' if current_env == "prd" else "testing"  -- This value will come from dynamodb directly

    logger.info(f"HTTP Request Info for status check--> {pick_export_config['api_parameter']}")
    http_client = HTTPClient(pick_export_config)


    # calling the API and if token is expired , it will generate the token again and retry
    workflow_publish_status_resp, api_status, status_code = token_retry_mechanism(
        bucket_name, http_client)
    
    logger.info(workflow_publish_status_resp)
    logger.info(api_status)
    logger.info(status_code)

    if workflow_publish_status_resp is None or status_code is None or not api_status:
        err_msg = f"STATUS API from ERM is failed for the voucher - {workflow_publish_status_resp=}"
        logger.info(err_msg)
        return False, err_msg

    publish_status = get_publish_status(workflow_publish_status_resp, api_status, status_code, status_root_tag)
    logger.info(f"Import status - {publish_status}")
    # Checking the status, if it's processing it will wait and retry until status_timeout parameter
    is_api_timedout = False
    max_retries = pick_export_config['job_parameter']['max_retry']
    delay = pick_export_config['job_parameter']['status_check_delay'] # delay in seconds
    if publish_status.upper() == "INPROGRESS":

        time.sleep(delay)
        for attempt in range(1, max_retries + 1):
            workflow_publish_status_resp, api_status, status_code = token_retry_mechanism(
                bucket_name, http_client)
            
            if not api_status or not workflow_publish_status_resp:
                err_msg = f"STATUS API from ERM is failed for the voucher - {workflow_publish_status_resp=}"
                logger.info(err_msg)
                return False, err_msg

            publish_status = get_publish_status(workflow_publish_status_resp, api_status, status_code, status_root_tag)
            logger.info(f"Import status - {publish_status}")

            if publish_status.upper() != "INPROGRESS":
                logger.info(f"Status is {workflow_publish_status_resp}. Proceeding with the next step.")
                break
            
            if attempt < max_retries:
                logger.info(f" Attempt- {attempt} finished. Status is 'Processing'. Trying again after {delay} seconds.")
                time.sleep(delay)  # Wait before retrying
            else:
                err_msg = f"Status API for voucher-{voucher_id} and id - {upload_key} failed after all retries. Response is {workflow_publish_status_resp}"
                logger.info(err_msg)
                logger.info(f"MOving to next voucher to process")
                return False, err_msg
    
    #workflow_publish_status_response = workflow_publish_status_resp.replace('"', "")
    
    
    # if API response is CompletedwithErrors then breaking the code and coming out of it.
    if publish_status.upper() == "COMPLETEDWITHERRORS" or publish_status.upper() == "FAILED":
        err_msg = f"Status API returned with error in processing at server for voucher - {voucher_id} with status - {publish_status} ans response is {workflow_publish_status_resp}"
        logger.error(err_msg)
        logger.info(f"MOving to next voucher to process")
        return False, err_msg
    
    imported_raw_uri = pick_export_config['job_parameter']['imported_raw_data_path']
    not_imported_raw_uri = pick_export_config['job_parameter']['un_imported_raw_data_path']
    voucher_processed_path = pick_export_config['job_parameter']['processed_status_path']
    voucher_processed_path = voucher_processed_path.format(project_id=query_id)
    voucher_processed_path = f"s3://{bucket_name}/{voucher_processed_path}/{partition_str}"
    # if API response is Completed then moving ahead with next steps
    is_imported = True if publish_status.upper() == "IMPORTED" else False
    status_raw_folder = imported_raw_uri if publish_status.upper() == "IMPORTED" else not_imported_raw_uri
    
    logger.info(f" voucher - {voucher_id} processed in server with status - {publish_status}")
    
    response_signed_url_json = workflow_publish_status_resp
    request_status_check = json.loads(workflow_publish_status_resp) if isinstance(workflow_publish_status_resp, str) else workflow_publish_status_resp

    if process_name == 'DRL':
        for drl_info in request_status_check.get(status_root_tag):
            data_collection_drl.append({
                "DRL_UPLOAD_KEY": upload_key,
                "DRL_HDR_NO": drl_info['DRL_HDR_NO'],
                "DRL_ITEM_ID": drl_info['DRL_ITEM_ID'],
                "VOUCHERNO": drl_info['VOUCHERNO'],
                "VOUCHERITEMNO": drl_info['VOUCHERITEMNO'],
                "DRLPOSTINGSTATUS": drl_info['STAT'],
                "DRLERRORDESCRIPTION": drl_info['ATTRIB_1'] + ' .' + drl_info['RESOLVED_MSG'] if drl_info['STAT'].upper() != 'IMPORTED' else ''
            })
    else:
        for picklist_info in request_status_check.get(status_root_tag):
            data_collection_drl.append({
                "PICKLISTPOSTINGSTATUS": picklist_info['STAT'],
                "PICKLISTERRORDESCRIPTION": picklist_info['ATTRIB_1'] + ' .' + picklist_info['RESOLVED_MSG'] if picklist_info['STAT'].upper() != 'IMPORTED' else ''
            })


    # Current date and time
    now = datetime.now()
    # Formatting date and time
    formatted_now = now.strftime("%Y-%m-%d-%H-%M-%S")
    logger.info("formatted_now --> " + str(formatted_now))
    status_raw_folder = status_raw_folder.format(project_id=query_id,voucher_to_process=voucher_id)
    object_name = f"{status_raw_folder}/{upload_key}_{formatted_now}.json"
    #upload json file to temp folder in raw bucket
    s3_upload_status = False
    kms_key_id = drl_export_config['job_parameter']['kms_key_id']
    json_data = json.dumps(response_signed_url_json)
    if s3_client.upload_to_s3(json_data, object_name, kms_key_id, is_gzip=False):
        logger.info(f"Uploaded ERM {process_name} raw file info to {object_name}")
        s3_upload_status = True
    else:
        logger.error("Failed to upload extrated json raw file to S3")
        s3_upload_status = False

    return is_imported, data_collection_drl


full_incr = 'f'

try:
    
    erm_min_df = None
    if is_depenednt_catalog:
        logger.info("redshift data access started")

        logger.info("Pulling the records of current RIL processing file.")

        primary_src_query = "select rml.min_number AS VOUCHER_NO from worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.ril_min_list"
        ril_src_min_df = spark.sql(primary_src_query)
        logger.info(f"Total Record Count from source - {ril_src_min_df.count()}")
        

        transformed_query = f""" 
        SELECT pj.proj_id AS PROJECT_ID,
            stm.site_mto_hdr_id AS SITEMTOHDRID,
            date_format(to_date(rml.issued_date, 'yyyy-MM-dd'), 'dd.MM.yyyy') AS PRODUCTIONDATE,
            rml.pts_code AS MATERIALID,
            rml.contractor_name AS SUBCONTRACTOR,
            rml.final_qty AS QUAN,
            rml.uom AS UOM,
            smi.line_no AS SITEMTOLINEITEMID,
            '1311-1' AS STORE,
            dd.DELIV_DESIG_ID AS FABCAT,
            '1311-1' AS LOCATION,
            rml.min_number AS VOUCHERNO,
            ROW_NUMBER() OVER (PARTITION BY rml.min_number ORDER BY smi.line_no) AS VOUCHERITEMNO,
            'INS' as TRANSTYPE
        FROM worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.ril_min_list rml --Table contains data from the source excel file
        JOIN worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.draw d
        ON d.draw_name = rml.drawing_number
        AND d.is_current = 1
        JOIN worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.site_mto_hdr stm
        ON d.draw_no = stm.draw_no
        AND stm.is_current = 1
        JOIN worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.site_mto_item smi
        ON smi.site_mto_hdr_no = stm.site_mto_hdr_no
        AND smi.is_current = 1
        JOIN worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.mat mat
        ON mat.mat_id = rml.pts_code
        AND smi.mat_no = mat.mat_no
        AND mat.is_current = 1
        JOIN worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.proj pj
        ON pj.proj_no = stm.proj_no
        AND pj.is_current = 1
        JOIN worley_datalake_sydney_{current_env}_glue_catalog_database_supply_chain_erm_curated.deliv_desig dd
        ON dd.deliv_desig_no = stm.deliv_desig_no
        AND dd.is_current = 1
        WHERE pj.proj_id = '{project_id_name}';

        """
        erm_min_df = spark.sql(transformed_query)
        # erm_min_df = joined_df.select(*requrired_api_data_columns)
        erm_min_df.printSchema()
        erm_min_df.show()

        logger.info("redshift data ended secess")
    else:
        dependent_path = bucket_name+'/'+ dependent_input_path + depenedent_data_local_sample_file_name
        if full_incr == 'f':
            dependent_path = bucket_name+'/'+ dependent_input_path + depenedent_data_local_full_load_file_name
        logger.info(f"s3 input path - {dependent_path}")
        
        erm_min_dynf=glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [f"s3://{dependent_path}"]},
            format="csv",
            format_options={
                "withHeader": True,
                "optimizePerformance": True,
                
            },
        )
        
        erm_min_df = erm_min_dynf.toDF()

    if erm_min_df and erm_min_df.count() < 1:
        logger.info("No data to process so exiting gracefully")
        os._exit(os.EX_OK)
    erm_min_df.printSchema()

    # Finding if any data is missing from source table for processing

    vouchers_missing = ril_src_min_df.join(erm_min_df, erm_min_df.VOUCHERNO == ril_src_min_df.VOUCHER_NO, "leftanti").select(col(ril_src_min_df.VOUCHER_NO)).collect()

    cols_name = ['VOUCHERNO', "VOUCHERITEMNO", 'PRODUCTIONDATE', 'DRL_UPLOAD_KEY',
                                    "DRL_HDR_NO", "DRL_ITEM_ID", "DRLPOSTINGSTATUS", "DRLERRORDESCRIPTION", "PICKLISTPOSTINGSTATUS", "PICKLISTERRORDESCRIPTION"]
    
    if len(vouchers_missing) > 0:
        vouchers_missing_info = [voucher.VOUCHER_NO for voucher in vouchers_missing]
        logger.info(f"Missing voucher from source are - {vouchers_missing_info}")
        voucher_missing_dict = {'VOUCHERNO': vouchers_missing_info}
        main_resultant_notification_df = pd.DataFrame.from_dict(voucher_missing_dict)
        for cols in cols_name:
            if cols != "VOUCHERNO":
                if "ERRORDESCRIPTION" in cols:
                    main_resultant_notification_df[cols] = "Voucher is missing from query run. Check the derived tables in query like draw no, deliv_desig_no, etc. data may be missing."
                elif "POSTINGSTATUS" in cols:
                    main_resultant_notification_df[cols] = "MISSING"
                else:
                    main_resultant_notification_df[cols] = ""
    else:
        main_resultant_notification_df = pd.DataFrame(columns=cols_name)


    # # Trim all string columns to remove leading/trailing spaces
    # for col_name in erm_min_df.columns:
    #     erm_min_df = erm_min_df.withColumn(col_name, trim(erm_min_df[col_name]))

    # logger.info("Checking for spaces in material id - 1")
    # erm_min_df.show()
        
    # # Remove \n and \r characters from all string columns
    # for col_name in erm_min_df.columns:
    #     # Trim spaces, Remove newlines, Replace multiple spaces with a single space
    #     # Convert all columns to STRING to prevent formatting issues
    #     erm_min_df = erm_min_df.withColumn(
    #         col_name,
    #         expr(f"trim(regexp_replace(cast({col_name} as string), '[\\n\\r]+', ' '))")
    #     )
    

    # Remove \n and \r characters from all string columns
    # Trim spaces, Remove newlines, Replace multiple spaces with a single space
    # Convert all columns to STRING to prevent formatting issues

    erm_min_df = erm_min_df.select([regexp_replace(col(col_name).cast(StringType()), '[\\n\\r]+', ' ').alias(col_name) if col_name.upper() != "MATERIALID" else col(col_name).cast(StringType()).alias(col_name) for col_name in erm_min_df.columns])
    
    logger.info("Checking for spaces in material id")
    erm_min_df.show()

    min_df = erm_min_df.select("VOUCHERNO").distinct()
    #grouped_df = param_df.groupby('voucherno').agg(concat_ws('\n', collect_list("combined_param")).alias("combined_param"))
    #grouped_df.show()
    
    if min_df and min_df.count() < 1:
        logger.info("No data to process so exiting gracefully")
        os._exit(os.EX_OK)
    
    report_api_endpoint = erm_export_config['api_parameter']['endpoint']
    
    for voucher_data in min_df.collect():
        voucher_to_process = voucher_data["VOUCHERNO"]

        #voucher_to_process = '13135496'
        #voucher_to_process = selected_voucher
        logger.info(f"voucher - {voucher_to_process}")
        
        select_df = erm_min_df.where(erm_min_df.VOUCHERNO==f'{voucher_to_process}')
        #select_df = select_df.withColumn("fabcat",lit("1"))
        #select_df = select_df.withColumn("location",lit("FY_BIN_001"))
        select_df.show()
    
        # Convert all column names to uppercase
        for col_name in select_df.columns:
            select_df = select_df.withColumnRenamed(col_name, col_name.upper())

        select_pdf = select_df.toPandas()
        # Apply final cleaning in Pandas (Ensure no hidden newlines)
        select_df_pd = select_pdf.applymap(lambda x: x.replace("\n", " ").replace("\r", " ") if isinstance(x, str) else x)
        # Validate Data - Ensure No Newlines Exist in Any Field Before Writing CSV
        for col in select_df_pd.columns:
            assert not select_df_pd[col].str.contains("\n").any(), f"Newline found in column {col}!"
        # Log and Verify If Newlines Exist After Converting to Pandas**
        logger.info("Checking for newlines in Pandas DataFrame:")
        for col in select_df_pd.columns:
            if select_df_pd[col].str.contains("\n").any():
                logger.error(f"Newline found in column {col}!")

        top_pdf = select_df_pd

        logger.info(f"voucher - {voucher_data} processing Started--------------------------------------------")

        # data_buffer = io.StringIO()
        # top_pdf.to_csv(
        #     data_buffer, 
        #     sep=";", 
        #     index=False , 
        #     lineterminator="\n" , 
        #     quoting=0, 
        #     escapechar="\\",
        #     mode="w",  # Ensure Pandas writes everything in a single pass
        #     errors="replace"  # Replace invalid characters instead of breaking lines
        # )
        # api_data_csv_strng = data_buffer.getvalue()
        # data_buffer.close()  # Free memory
        # # Cleanup to Remove Any Hidden Issues in `csv_string`
        # api_data_csv_strng = api_data_csv_strng.replace("\n\n", "\n")  # Remove accidental blank lines
        # api_data_csv_strng = api_data_csv_strng.replace("\r\n", "\n")  # Normalize Windows-style newlines
        # api_data_csv_strng = api_data_csv_strng.strip()  # Ensure no leading/trailing spaces

        lines = top_pdf.apply(lambda x: ';'.join(x.astype(str)), axis=1)
        header = ';'.join(top_pdf.columns)
        api_data_csv_strng = header + os.linesep + os.linesep.join(lines)

        import_status, data_info = process_request_creation_status_updates(
            process_name="DRL",
            voucher_data=voucher_data, 
            api_data_csv_strng=api_data_csv_strng,
            erm_export_config=erm_export_config, 
            data_export_config=data_export_metadata, 
            bucket_name=bucket_name, 
            query_id=drl_query_id, 
            status_root_tag=register_status_root_tag, 
            partition_str=partition_str, 
            s3_client=s3_client
        )

        if import_status:

            post_voucher_df = select_df_pd[["PRODUCTIONDATE", "MATERIALID", "VOUCHERNO", "VOUCHERITEMNO"]]
            drl_data_results_df = pd.DataFrame(data_info)

            drl_creation_status_df = pd.merge(post_voucher_df, drl_data_results_df,
                     left_on=["VOUCHERNO", "VOUCHERITEMNO"],
                      right_on=["VOUCHERNO", "VOUCHERITEMNO"], how='inner', suffixes=('', '_PL'))
            
            drl_creation_status_df = drl_creation_status_df[['VOUCHERNO', "VOUCHERITEMNO", 'PRODUCTIONDATE', 'DRL_UPLOAD_KEY',
                                    "DRL_HDR_NO", "DRL_ITEM_ID", "DRLPOSTINGSTATUS", "DRLERRORDESCRIPTION"]]
        else:
            if not isinstance(data_info, list):
                drl_error_df = pd.DataFrame([{"DRLPOSTINGSTATUS": "FAILED", "DRLERRORDESCRIPTION": data_info}])
            else:
                post_voucher_df = select_df_pd[["PRODUCTIONDATE", "SITEMTOLINEITEMID", "VOUCHERNO", "VOUCHERITEMNO"]]
                post_voucher_df = post_voucher_df.rename(columns={'SITEMTOLINEITEMID': 'DRL_ITEM_ID'})
                drl_error_df = pd.DataFrame([{"DRLPOSTINGSTATUS": data_info[0]['DRLPOSTINGSTATUS'], "DRLERRORDESCRIPTION": data_info[0]['DRLERRORDESCRIPTION']}])

            drl_creation_status_df = post_voucher_df.merge(drl_error_df, how='cross')
            drl_creation_status_df['DRL_UPLOAD_KEY'] = ""
            drl_creation_status_df['DRL_HDR_NO'] = ""


        if import_status:
            # Convert json response into df
            json_df = pd.DataFrame(data_info)
            #Merge json_df and select_df
            json_df.columns = json_df.columns.str.lower()
            select_df_pd.columns = select_df_pd.columns.str.lower()
            columns_to_select_for_req=['project_id','quan','uom','store','location','voucherno','voucheritemno']
            merged_df = select_df_pd[columns_to_select_for_req].merge(
                json_df,
                on=['voucherno', 'voucheritemno'],  # specify the columns to join on
                how='left',  # specify the type of join
                suffixes=('', '_audit')  # handle duplicate column names if any
            )
            
            # Create csv payload
            output_columns=['project_id','drl_hdr_no','drl_item_id','quan','uom','store','location']
            merged_df = merged_df[output_columns]
            
            # Convert float values with `.0` to int while keeping other floats unchanged that is coming from status API as floats
            cols_to_convert = ['drl_hdr_no', 'drl_item_id']
            merged_df[cols_to_convert] = merged_df[cols_to_convert].applymap(lambda x: int(x) if not isinstance(x, str) else x)
            merged_df = merged_df.rename(columns={'project_id': 'projectid', 'drl_hdr_no': 'drl_no'})
            merged_df.columns = merged_df.columns.str.upper()
            csv_buffer = io.StringIO()

            logger.info(f"Register Process processing Started--------------------------------------------")

            # #select_df_pd.show()
            # merged_df.to_csv(
            #     csv_buffer, 
            #     sep=";", 
            #     index=False , 
            #     lineterminator="\n" , 
            #     quoting=0, 
            #     escapechar="\\",
            #     mode="w",  # Ensure Pandas writes everything in a single pass
            #     errors="replace"  # Replace invalid characters instead of breaking lines
            # )
            # api_data_csv_string = csv_buffer.getvalue()
            # csv_buffer.close()  # Free memory
            # # Cleanup to Remove Any Hidden Issues in `csv_string`
            # api_data_csv_string = api_data_csv_string.replace("\n\n", "\n")  # Remove accidental blank lines
            # api_data_csv_string = api_data_csv_string.replace("\r\n", "\n")  # Normalize Windows-style newlines
            # api_data_csv_string = api_data_csv_string.strip()  # Ensure no leading/trailing spaces
            
            # #logger.info(f"voucher - {voucher_data} and api data - {api_data_csv_strng}")

            lines = merged_df.apply(lambda x: ';'.join(x.astype(str)), axis=1)
            header = ';'.join(merged_df.columns)
            api_data_csv_string = header + os.linesep + os.linesep.join(lines)

            import_status, data_info = process_request_creation_status_updates(
                process_name="PICK_LIST",
                voucher_data=voucher_data, 
                api_data_csv_strng=api_data_csv_string,
                erm_export_config=erm_export_config, 
                data_export_config=data_export_metadata, 
                bucket_name=bucket_name, 
                query_id=picklist_query_id, 
                status_root_tag=pick_status_root_tag, 
                partition_str=partition_str, 
                s3_client=s3_client
            )

            if import_status:

                picklist_data_results_df = pd.DataFrame(data_info)

                drl_picklist_creation_status_df = drl_creation_status_df.merge(picklist_data_results_df, how='cross')

                vouchers_processed.append(voucher_to_process)
                
            else:

                if not isinstance(data_info, list):
                    picklist_error_df = pd.DataFrame([{"PICKLISTPOSTINGSTATUS": "FAILED", "PICKLISTERRORDESCRIPTION": data_info}])
                else:
                    picklist_error_df = pd.DataFrame(data_info)


                drl_picklist_creation_status_df = drl_creation_status_df.merge(picklist_error_df, how='cross')

                vouchers_failed.append(voucher_to_process)

            drl_picklist_creation_status_df['AWSExecutionDate'] = pd.Timestamp.now()

            

        else:
            logger.info("As the status is Not Imported So not pick API is not sent")
            drl_creation_status_df['AWSExecutionDate'] = pd.Timestamp.now()

            picklist_error_df = pd.DataFrame([{"PICKLISTPOSTINGSTATUS": "NOT STARTED", "PICKLISTERRORDESCRIPTION": "DRL IMPORT was not successful to run PICKLIST creation"}])

            drl_picklist_creation_status_df = drl_creation_status_df.merge(picklist_error_df, how='cross')

            vouchers_failed.append(voucher_to_process)
        
        main_resultant_notification_df = pd.concat([main_resultant_notification_df, drl_picklist_creation_status_df], ignore_index=True)

        
    
    processed_msg = f"Successfully Processed list of vouchers are - {vouchers_processed}"
    logger.info(f"{processed_msg}")

    source_sys = erm_export_config['SourceSystemId']
    target_sys = erm_export_config['MetadataType']
    functional_area = "DRL_PICKLIST_CREATION"
    request_run_id = job_run_id
    success_reason = processed_msg
    if len(vouchers_failed) > 0:
        failed_msg = f"Failed to Process list of vouchers are - {vouchers_failed}"
        logger.info(f"{failed_msg}")
        subject, email_body = generate_failure_email_body(current_env, source_sys, target_sys, functional_area, request_run_id, (vouchers_processed, vouchers_failed), failed_msg, interface_name='')
    else:
        subject, email_body = generate_success_email_body(current_env, source_sys, target_sys, request_run_id, datetime.now(timezone.utc), success_reason, interface_name='')


    final_notify_data_buffer = io.StringIO()

    main_resultant_notification_df.to_csv(
        final_notify_data_buffer, 
        sep=",", 
        index=False , 
        lineterminator="\n" , 
        quoting=0, 
        escapechar="\\",
        mode="w",  # Ensure Pandas writes everything in a single pass
        errors="replace"  # Replace invalid characters instead of breaking lines
    )
    file_content = final_notify_data_buffer
    s = datetime.now(timezone.utc)
    fmrt_tmt = datetime.strftime(s, '%Y%m%d%H%M%S')

    file_name = f'ril_min_list_status_{fmrt_tmt}.csv'
    send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port, file_content=file_content, file_name=file_name, attach_file=True)
    
except Exception as e:
    logger.error(e)
    logger.info("Error on line {}".format(sys.exc_info()[-1].tb_lineno))

    logger.error(f"An error occurred: {str(e)}", exc_info=True)
    error_message = f"Error: {str(e)}\nTraceback:\n{traceback.format_exc()}"
    logger.error(error_message)

    source_sys = erm_export_config['SourceSystemId']
    target_sys = erm_export_config['MetadataType']
    functional_area = "DRL_PICKLIST_CREATION"
    request_run_id = job_run_id
    subject, email_body = generate_failure_email_body(current_env, source_sys, target_sys, functional_area, request_run_id, (vouchers_processed, vouchers_failed), error_message, interface_name='')

    if main_resultant_notification_df.shape[0] >= 1:
        final_notify_data_buffer = io.StringIO()

        main_resultant_notification_df.to_csv(
            final_notify_data_buffer, 
            sep=",", 
            index=False , 
            lineterminator="\n" , 
            quoting=0, 
            escapechar="\\",
            mode="w",  # Ensure Pandas writes everything in a single pass
            errors="replace"  # Replace invalid characters instead of breaking lines
        )
        file_content = final_notify_data_buffer

        s = datetime.now(timezone.utc)
        fmrt_tmt = datetime.strftime(s, '%Y%m%d%H%M%S')

        file_name = f'ril_min_list_status_{fmrt_tmt}.csv'
        send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port, file_content=file_content, file_name=file_name, attach_file=True)
    else:
        send_email(sender_email, receiver_email, subject, email_body, smtp_server, smtp_port, file_content=None, file_name=None, attach_file=False)


    # send the notifications
    # sys.exit(1)

job.commit()
