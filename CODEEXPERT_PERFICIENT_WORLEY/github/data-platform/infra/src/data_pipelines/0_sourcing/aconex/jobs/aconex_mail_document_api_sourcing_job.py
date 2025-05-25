import sys
import json
import base64
import boto3
import os
import requests
from pyspark.sql import DataFrame
from pyspark.context import SparkContext
import awswrangler as wr
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from worley_helper.utils.helpers import (
    get_partition_str_mi,
    write_glue_df_to_s3_with_specific_file_name,
    save_spark_df_to_s3_with_specific_file_name
    )
from worley_helper.utils.logger import get_logger
from worley_helper.utils.date_utils import (
    generate_timestamp_string,
    generate_today_date_string,
    generate_formatted_snapshot_date_string
    )
from worley_helper.utils.constants import (
    TIMEZONE_SYDNEY,
    DATETIME_FORMAT,
    REGION,
    DATE_FORMAT,
    AUDIT_DATE_COLUMN,
    TIMEZONE_UTC,
    TIMESTAMP_FORMAT
    )
from worley_helper.utils.aws import get_secret, S3, DynamoDB
from pyspark.sql.functions import lit, col, when, explode, broadcast
import awsglue.gluetypes as gt
from pyspark.sql.types import StructType, ArrayType, StringType, StructField

from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import datetime
import pytz

MAX_WORKERS = 100
FOLDER_TIMESTAMP_FORMAT: str = "%Y-%m-%d-%H-%M-%S"



def get_raw_downloaded_files(raw_bckt,tmp_files_path):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(raw_bckt)
    count = 0
    for object in bucket.objects.filter(Prefix=tmp_files_path):
        if object.key.endswith('.xml'):
            count += 1
    print(f" {count} total count in {tmp_files_path}")
    return count

class ThreadHandler(threading.Thread):
    def __init__(self, task_id, task_func, *task_args, **task_kwargs):
        super().__init__()
        self.task_id = task_id
        self.task_func = task_func
        self.task_args = task_args
        self.task_kwargs = task_kwargs
        self._status = 'Initialized'
        self._exception = None
        # self.logger = logger

    def run(self):
        try:
            self._set_status('Running')
            logger.info(f"Task {self.task_id} started.")

            # Execute the task function with the provided arguments
            self.task_func(*self.task_args, **self.task_kwargs)

            self._set_status('Completed')
            logger.info(f"Task {self.task_id} completed successfully.")
        except Exception as e:
            self._exception = e
            self._set_status('Failed')
            logger.info(f"Error - Task {self.task_id} encountered an error: {e}")

    def _set_status(self, status):
        self._status = status

    def get_status(self):
        return self._status

    def has_failed(self):
        return self._exception is not None

    def get_exception(self):
        return self._exception


def extract_content(response, response_type, status_code):
    SUCCESS = True
    FAILURE = False
    status_code = status_code
    try:
        content_extractors = {
            'json': lambda: response.json(),
            'binary': lambda: response.content,
            'xml': lambda: response.content,
            'text': lambda: response.text,
            'raw': lambda: response
        }
        content_extractor = content_extractors.get(response_type, response.json)
        return content_extractor(), SUCCESS, status_code
    except Exception as e:
        logger.info(f"Error extracting content: {e}")
        return None, FAILURE , status_code


def call_api(api_endpoint, username, password, query_params, api_timeout):
    
    # Set headers
    authData = f"{username}:{password}"
    base64AuthData = base64.b64encode(authData.encode()).decode()
    authHeaders = {
        "Authorization": f"Basic {base64AuthData}",
        "Content-Type": "application/xml",
    }

    # Log API request
    logger.info(f"API call made to {api_endpoint}")
    retry_attempt = 1
    MAX_RETRIES = 2
    while retry_attempt <= MAX_RETRIES:
        try:
            response = requests.get(
                api_endpoint,
                params=query_params,
                headers=authHeaders,
                timeout=api_timeout,
                verify=False,
            )
            # Extract json content from the response
            logger.info(f"Validate the response and Extract json content and write to file")
            status_code = response.status_code
            if 200 <= status_code < 300:
                logger.info(f"API call made to {api_endpoint} completed, Status code: {response.status_code}")
                response, status, status_code = extract_content(response, 'xml', status_code)
                return response, status, status_code
            else:
                logger.error(f"API call to {api_endpoint} failed with status code: {status_code}")
                return None, False, status_code
        except requests.exceptions.Timeout as e:
            # Retry for any other error
            logger.info(f"Error in calling Project list API: {e}")
            if retry_attempt == MAX_RETRIES:
                logger.error(f"Maximum retries of {MAX_RETRIES} done so exiting with timeout error")
                raise  # Indicates other error
            logger.info(f"Request timed out. Retrying...")
            retry_attempt=retry_attempt+1
            api_timeout = api_timeout+60
            
        except requests.exceptions.RequestException as e:
            logger.info(f"Error in calling Export API: {e}")
            if response.status_code in [401, 403]:
                # If authentication failure, refresh token and retry
                logger.info(f"Received {response.status_code} error. Refreshing token and retrying...")
                return None, False, response.status_code  # Indicates authentication failure
            else:
                # Retry for any other error
                logger.info(f"Received {response.status_code} error. Retrying...")
                raise
        except Exception as e:
            # Handle all other exceptions
            logger.info(f"An error occurred: {e}")
            raise  # Indicates other error



def get_mail_document_data(
        api_endpoint,
        username,
        password,
        api_timeout,
        api_tmp_folder_path,
        output_file_name
):
    try:
        mail_doc_data, api_status, status_code = call_api(api_endpoint, username, password, query_params={}, api_timeout=api_timeout)

        if api_status and mail_doc_data:
            object_name = f"{api_tmp_folder_path}/{output_file_name}"
            if s3_client.upload_to_s3(mail_doc_data, object_name, kms_key_id, is_gzip=True):
                logger.info(f"Uploaded mail response info to {object_name}")
                return True
            else:
                logger.info(f"Upload failed mail response info to {object_name}")
                return False
        else:
            logger.info(f"API failed mail for endpoint {api_endpoint}")
            return False
    except Exception as e:
        logger.info(f"Error in api responce process - {e}")
        raise


def run_parallel_threads(threads, max_workers=MAX_WORKERS):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_thread = {executor.submit(thread.run): thread for thread in threads}

        for future in as_completed(future_to_thread):
            thread = future_to_thread[future]
            try:
                future.result()  # This will re-raise any exception caught during execution
            except Exception as exc:
                logger.info(f'Error - {thread.task_id} generated an exception: {exc}')
            finally:
                if thread.has_failed():
                    logger.info(f'Error - {thread.task_id} failed with exception: {thread.get_exception()}')


def get_acconex_mails_document_data(project_mail_list, mail_document_config):
    api_threads = []
    api_endpoint = mail_document_config['api_parameter']['endpoint']
    tmp_object_name = mail_document_config['job_parameter']['tmp_object_name']
    api_timeout = mail_document_config['api_parameter']['api_timeout']
    if not api_timeout:
        api_timeout = 60
    logger.info("start of API")
    for project_mail_id in project_mail_list:
        formated_url = api_endpoint.format(
            instance_host=endpoint_host,
            project_id=project_id,
            mail_id=project_mail_id
        )
        formated_file_name = tmp_object_name.format(
            mail_id=project_mail_id,
            current_batch_run_time=current_batch_run_time_str
        )
        api_task_id = f"{project_id}_{project_mail_id}_{current_batch_run_time_str}"
        logger.info(f"valid url- {formated_url}")
        logger.info(f"valid file_name - {formated_file_name}")
        # api_process_status = get_mail_document_data(
        api_threads.append(
            ThreadHandler(
                task_id=api_task_id,
                task_func=get_mail_document_data,
                api_endpoint=formated_url,
                username=username,
                password=password,
                api_timeout=api_timeout,
                api_tmp_folder_path=tmp_formated_batch_run_folder_path,
                output_file_name=formated_file_name
            )
        )
        logger.info(f"api response status for {project_mail_id} thread added")
        # status = api_process_status

    return api_threads


def process_mail_document_data(
        tmp_formated_batch_run_folder_path: str,
        relationalized_data_path: str,
        s3_bckt: str,
        req_extract_schema,
        renamed_cols,
        **kwargs):
    FAILURE = False
    logger.info(tmp_formated_batch_run_folder_path)
    source_name = kwargs['source_name']
    function_name = kwargs['function_name']
    s3_output_raw_path = kwargs['raw_final_out']
    row_tag = kwargs['row_tag']
    root_tag = kwargs['root_tag']
    ip_schema = StructType([
        StructField("Attachments", StructType([
            StructField("RegisteredDocumentAttachment", ArrayType(StructType([
                StructField("Confidential", StringType()),
                StructField("DocumentId", StringType()),
                StructField("RegisteredAs", StringType()),
                StructField("DocumentNo", StringType()),
                StructField("FileName", StringType()),
                StructField("FileSize", StringType()),
                StructField("Revision", StringType()),
                StructField("RevisionDate", StringType(), True),
                StructField("Status", StringType()),
                StructField("Title", StringType()),
                StructField("_attachmentId", StringType())
                ])))
            ])),
        StructField("_MailId", StringType())
        ])
    '''
    src_mapped_df = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [
                f"{tmp_formated_batch_run_folder_path}"
            ],
            "recurse": True
        },
        format="xml",
        format_options={"rowTag": root_tag, "withSchema": json.dumps(req_extract_schema.jsonValue()) },
        transformation_ctx=f"{function_name}"
    )'''
    scema_dt = type(ip_schema)
    print(f"scema datatype - {scema_dt}")
    mail_doc_df = spark.read \
    .format("com.databricks.spark.xml") \
    .option("rowTag", root_tag) \
    .option("mode", "PERMISSIVE") \
    .option("recursiveFileLookup", "true") \
    .option("maxFilesPerTrigger", 1000) \
    .option("nullValue","") \
    .option("treatEmptyValuesAsNulls","true") \
    .load(tmp_formated_batch_run_folder_path,schema=ip_schema)
    src_mapped_df = DynamicFrame.fromDF(mail_doc_df, glueContext, function_name)
    tf_ctxt = source_name + function_name
    logger.info("data gathered")
    logger.info("count for repartition started----")
    total_count = src_mapped_df.count()
    logger.info(f" source total count from raw s3 {total_count}")
    if total_count < 1:
        logger.info(f" 0 source total count from raw s3")
        return FAILURE, total_count
    if total_count > 15000: # How to process large volume of data
        meta_num_partitions = (
            200  # Adjust this value based on your data size and cluster resources
            )
        src_mapped_df = src_mapped_df.repartition(meta_num_partitions)      
    logger.info("pure relationalize on top of df start --")
    df_relationalized = src_mapped_df.relationalize(
        "root", relationalized_data_path
    )
    logger.info(df_relationalized.keys())
    rel_tables = {}
    for rel_name_df in df_relationalized.keys():
        if rel_name_df != '':
            rel_tables[rel_name_df] = df_relationalized.select(rel_name_df).toDF()
    logger.info("post relationalize tables below")
    logger.info(rel_tables)

    rel_final_df = df_relationalized.select('root')

    rel_final_df = rel_final_df.toDF()
    rel_final_df = rel_final_df.withColumnRenamed("Attachments.RegisteredDocumentAttachment",
                                                  "Attachments_RegisteredDocumentAttachment")

    fin_df = df_relationalized.select('root_Attachments.RegisteredDocumentAttachment')
    fin_df = fin_df.toDF()
    if fin_df.count() < 1:
        logger.info(f" 0 documents from source total count from raw s3")
        return True, 0
    
    final_df = fin_df.join(broadcast(rel_final_df), fin_df.id == rel_final_df.Attachments_RegisteredDocumentAttachment,
                           "left")
    logger.info("post join schema")
    #if "Attachments.RegisteredDocumentAttachment.val.RegisteredAs" not in final_df.columns:
    #    final_df = final_df.withColumn("Attachments.RegisteredDocumentAttachment.val.RegisteredAs", lit(None).cast("string"))
    #if "Attachments.RegisteredDocumentAttachment.val.DocumentId" not in final_df.columns:
    #    final_df = final_df.withColumn("Attachments.RegisteredDocumentAttachment.val.DocumentId", lit(None).cast("string"))
    # Iterate through columns and handle missing columns
    for src_col, dest_col in renamed_cols.items():
        if not src_col in final_df.columns:
            final_df = final_df.withColumn(src_col, lit(None).cast("string"))
    
    #final_df.printSchema()
    logger.info("post process final schema")
    mail_doc_modif_df = final_df.select(
        [
            col("`{}`".format(str(src_col))).alias(rename_col)
            for src_col, rename_col in renamed_cols.items()
        ]
    )
    mail_doc_modif_df = mail_doc_modif_df.withColumn("project_id", lit(project_id))
    mail_doc_modif_df.printSchema()
    logger.info("pure relationalize on top of df end --")
    renamed_dynamicframe = DynamicFrame.fromDF(
        mail_doc_modif_df,
        glueContext,
        "dynamic_frame"
    )
    logger.info("count for repartition started----")
    total_count = renamed_dynamicframe.count()
    logger.info(f" source total count from raw s3 {total_count}")
    # 1 million records per partition
    num_partition = round(total_count / 1000000) + 1
    renamed_dynamicframe = renamed_dynamicframe.repartition(num_partition)
    logger.info("count for repartition end----")

    logger.info(f"post processing status success")

    logger.info(f"start of loading data into final raw output {s3_output_raw_path}")
    # Write partitioned data to S3
    success = write_glue_df_to_s3_with_specific_file_name(
        glueContext,
        renamed_dynamicframe,
        s3_bckt,
        s3_output_raw_path,
        function_name,
        typecast_cols_to_string=True
    )
    logger.info(f"Completed loading data into final raw output {s3_output_raw_path}")

    logger.info(f"start of loading sampling data into final raw --- ")
    # write samle data to s3 without partitioning
    sampling_fraction = float(
        kwargs['sampling_fraction'])
    sampling_seed = kwargs['sampling_seed']
    sampling_path = kwargs['sampling_path']
    sample_data = (
        renamed_dynamicframe.toDF()
        .sample(withReplacement=False,
                fraction=sampling_fraction,
                seed=sampling_seed)
    )
    # adjust the fraction as needed
    logger.info(f"Selected sample data {source_name}")
    success = write_glue_df_to_s3_with_specific_file_name(
        glueContext,
        DynamicFrame.fromDF(sample_data, glueContext, "sample_data"),
        s3_bckt,
        sampling_path,
        tf_ctxt
    )
    return success, total_count


# Init the audit date
audit_date_value = generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)

# Init the logger
logger = get_logger(__name__)

# Create a GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# Extract the arguments passed from the Airflow DAGS into Glue Job
args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "ProjectId",
        "domain_name",
        "source_name",
        "function_name",
        "metadata_table_name",
        "endpoint_host"
    ]
)

domain_name = args.get("domain_name")
project_id=args.get("ProjectId")
source_name = args.get("source_name")
function_name = args.get("function_name")
metadata_table_name = args.get("metadata_table_name")
endpoint_host=args.get("endpoint_host")
job_name = args["JOB_NAME"]
job_run_id = args["JOB_RUN_ID"]
# Get the current date
today = datetime.datetime.now(pytz.UTC)
current_batch_run_time = today.strftime(TIMESTAMP_FORMAT)
logger.info("Project ID ->"+project_id)
job.init(args["JOB_NAME"] + domain_name+ source_name + function_name+ project_id, args)
# Define the Sort Keys for DynamoDB Fetch
input_keys = "api#"  + domain_name + "#" + source_name + "#" + function_name
# Read Metadata
ddb = DynamoDB(metadata_table_name=metadata_table_name, default_region=REGION)
metadata = ddb.get_metadata_from_ddb(
    source_system_id=source_name, metadata_type=input_keys
)
    
logger.info(f" Metadata Response :{metadata}")

aconex_mail_document_config = metadata
logger.info(f" Aconex mail_document Config :{aconex_mail_document_config}")

#############  Load Aconex mail_document API Config ################
raw_bckt = aconex_mail_document_config['job_parameter']['bucket_name']
project_id_partition = f"Project={project_id}"
region = aconex_mail_document_config['aws_region']

endpoint = aconex_mail_document_config['api_parameter']['endpoint']

current_batch_run_time_str = generate_formatted_snapshot_date_string(
    current_batch_run_time,
    FOLDER_TIMESTAMP_FORMAT,
    TIMESTAMP_FORMAT
)

api_timeout = aconex_mail_document_config['api_parameter']['api_timeout']
api_tmp_folder = aconex_mail_document_config['job_parameter']['api_tmp_folder']
tmp_formated_batch_run_folder_path = api_tmp_folder.format(
    project_id=project_id,
    current_batch_folder_name=current_batch_run_time_str
)
aconex_mail_document_config['job_parameter']['api_tmp_folder'] = tmp_formated_batch_run_folder_path

relationalized_data_path = aconex_mail_document_config['job_parameter']['relation_tmp_folder']
relationalized_data_path = relationalized_data_path.format(
    project_id=project_id,
    current_batch_folder_name=current_batch_run_time_str
)
aconex_mail_document_config['job_parameter']['relation_tmp_folder'] = relationalized_data_path
batch_partition_path = get_partition_str_mi(
    input_date=current_batch_run_time,
    timezone=None,
    input_date_format=TIMESTAMP_FORMAT
)
parquet_data_path = aconex_mail_document_config['job_parameter']['output_s3']
parquet_data_path = parquet_data_path.format(
    project_id=project_id,
    batch_partition_path=batch_partition_path
)
aconex_mail_document_config['job_parameter']['output_s3'] = parquet_data_path

dependency_mail_id_output_folder_path = aconex_mail_document_config[
    'job_parameter']['dependency_mail_id_output_folder_path']
dependency_mail_id_output_folder_path = dependency_mail_id_output_folder_path.format(
    project_id=project_id,
    batch_partition_path=batch_partition_path
)
aconex_mail_document_config['job_parameter'][
    'dependency_mail_id_output_folder_path'] = dependency_mail_id_output_folder_path

dependency_full_load_mail_id_folder_path = aconex_mail_document_config[
    'job_parameter']['dependency_full_load_mail_id_folder_path']
dependency_full_load_mail_id_folder_path = dependency_full_load_mail_id_folder_path.format(
    project_id=project_id
)
aconex_mail_document_config['job_parameter'][
    'dependency_full_load_mail_id_folder_path'] = dependency_full_load_mail_id_folder_path

row_tag = aconex_mail_document_config['job_parameter']['row_tag']
root_name=aconex_mail_document_config['job_parameter']['root_tag']
row_child_name=aconex_mail_document_config['job_parameter']['row_child_tag']

sample_data_path = aconex_mail_document_config['job_parameter']['sampling_output_s3']
sampling_fraction = float(aconex_mail_document_config['job_parameter']['sampling_fraction'])
sampling_seed = aconex_mail_document_config['job_parameter']['sampling_seed']

auth_type = aconex_mail_document_config['api_parameter']['auth_type']
name=aconex_mail_document_config['name']
full_incr = aconex_mail_document_config['job_parameter']['full_incr']

temp_deletion_path = aconex_mail_document_config['job_parameter']['tmp_input_path']
temp_deletion_path = temp_deletion_path.format(
    project_id=project_id
)

api_retry_count = aconex_mail_document_config['api_parameter']['api_retry']

# Get the HTTP POST body parameters for the mail_document API
s3_client = S3(raw_bckt, region)

# Fetch base64 encoded username & password from Secrets Manager and add to AuthToken header
secret_param_key = json.loads(get_secret(aconex_mail_document_config['api_parameter']['secret_key'], region))

username=secret_param_key.get("username")
password=secret_param_key.get("password")

authData = f"{username}:{password}"
base64AuthData = base64.b64encode(authData.encode()).decode()

aconex_mail_document_config['api_parameter']['api_headers']['Authorization'] = f"{auth_type} {base64AuthData}"
mail_box_tag = "_MailId"
incremental_criteria_folder_location = aconex_mail_document_config[
    'job_parameter']['incremental_criteria_folder_location']

logger.info("Bucket Name -> " + raw_bckt)
logger.info("relationalized_data_path -> " + relationalized_data_path)
logger.info("parquet_data_path -> " + parquet_data_path)
logger.info("row_tag -> " + row_tag)
logger.info("endpoint -> " + str(endpoint))
mail_doc_project_audit_file = aconex_mail_document_config['job_parameter']['audit_file_name']
mail_doc_project_audit_file = mail_doc_project_audit_file.format(
    project_id=project_id
)
audit_full_path = f"{incremental_criteria_folder_location}/{mail_doc_project_audit_file}"

select_renamed_cols = {
    "Attachments.RegisteredDocumentAttachment.val.DocumentId": "DocumentId",
    "Attachments.RegisteredDocumentAttachment.val.DocumentNo": "DocumentNo",
    "Attachments.RegisteredDocumentAttachment.val.RegisteredAs": "RegisteredAs",
    "Attachments.RegisteredDocumentAttachment.val.FileName": "FileName",
    "Attachments.RegisteredDocumentAttachment.val.FileSize": "FileSize",
    "Attachments.RegisteredDocumentAttachment.val.Revision": "Revision",
    "Attachments.RegisteredDocumentAttachment.val.Status": "Status",
    "Attachments.RegisteredDocumentAttachment.val.Title": "Title",
    "Attachments.RegisteredDocumentAttachment.val._attachmentId": "attachmentId",
    "_MailId": "MailId"
}
select_renamed_cols = aconex_mail_document_config['job_parameter']['api_source_to_target_rename']
schema = gt.StructType([

    gt.Field("Attachments", gt.StructType([
        gt.Field("RegisteredDocumentAttachment", gt.ArrayType(gt.StructType([
            gt.Field("Confidential", gt.StringType()),
            gt.Field("DocumentId", gt.StringType()),
            gt.Field("RegisteredAs", gt.StringType()),
            gt.Field("DocumentNo", gt.StringType()),
            gt.Field("FileName", gt.StringType()),
            gt.Field("FileSize", gt.StringType()),
            gt.Field("Revision", gt.StringType()),
            gt.Field("RevisionDate", gt.StringType({}), {}),
            gt.Field("Status", gt.StringType()),
            gt.Field("Title", gt.StringType()),
            gt.Field("_attachmentId", gt.StringType())
        ])))
    ])),
    gt.Field("_MailId", gt.StringType({}), {})
])

#######################################################


success = False
kms_key_id = aconex_mail_document_config['job_parameter']['kms_key_id']
isDataProcessed = False

content, is_previous_run_done = s3_client.read_s3_file(
    incremental_criteria_folder_location,
    mail_doc_project_audit_file)

req_mailids_folder = dependency_mail_id_output_folder_path
# For Full load or whether it is first time load in incremental
# use all the mailids from the previous run as it is equivalent
# to Full load scenario
if full_incr.upper() == "F".upper():
    req_mailids_folder = dependency_full_load_mail_id_folder_path
else:
    if not is_previous_run_done:
        req_mailids_folder = dependency_full_load_mail_id_folder_path

project_mail_list = None
api_threads = []
mail_doc_job_start_time=generate_today_date_string(TIMEZONE_UTC,AUDIT_DATE_COLUMN)
is_glue_bookmark_enabled =  False
is_glue_bookmark_enabled = aconex_mail_document_config['job_parameter']['is_glue_bookmark_enabled']
project_mail_df = None
bkmrk_cntxt = aconex_mail_document_config['job_parameter']["bookmark_contxt"]
proj_bkmrk_cntxt = bkmrk_cntxt.format(
    project_id=project_id
    )
count_mailids = 0
processed_mail_count = 0
try:
    logger.info("start getting mailids")
    temp_path = f"s3://{raw_bckt}/{temp_deletion_path}"
    temp_path = s3_client.get_folder_path_from_s3(temp_path)    
    if is_glue_bookmark_enabled:
        logger.info(f"bookmrk enabled for s3://{raw_bckt}/{dependency_full_load_mail_id_folder_path}")
        mailid_dynf = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options = {
                "paths": [f"s3://{raw_bckt}/{dependency_full_load_mail_id_folder_path}"],
                "useS3ListImplementation":True,
                "recurse":True},
            format="parquet",
            transformation_ctx=f"{proj_bkmrk_cntxt}"
        )
        mailid_dynf.printSchema()
        mailid_df = mailid_dynf.toDF()
        if mailid_df.isEmpty():
            logger.info(f"No data to process so exiting gracefully")
            job.commit()
            os._exit(0)
        project_mail_df = mailid_df.select('_MailId').drop_duplicates().toPandas()
    else:
        project_mail_df = wr.s3.read_parquet(
            path=f"s3://{raw_bckt}/{req_mailids_folder}",
            columns=['_MailId'],
        )
    proj_dist_mails = []
    if not project_mail_df.empty:
        logger.info(f"processing to get dedup mailids")
        proj_dist_mails = project_mail_df['_MailId'].drop_duplicates().to_list()
        count_mailids = len(proj_dist_mails)
        pre_dedup_mails = len(project_mail_df)
        logger.info(f"prededup count {pre_dedup_mails} and post mailids count - {count_mailids}")
    isDataProcessed = False
    if proj_dist_mails and len(proj_dist_mails) > 0:

        #convert the mail list into batches of 5000
        max_batch_size=5000
        project_mail_list_batches = [proj_dist_mails[i:i + max_batch_size] for i in range(0,count_mailids, max_batch_size)]
        batch_number=0
        for mail_list_batch in project_mail_list_batches:
            batch_attempt=1
            batch_size=len(mail_list_batch)
            batch_number=batch_number+1
            logger.info(f"Batch {batch_number} started with count - {batch_size}")
            while batch_attempt <= 2:
                api_threads = get_acconex_mails_document_data(mail_list_batch,aconex_mail_document_config)
                if api_threads and len(api_threads) > 0:
                    run_parallel_threads(api_threads)

                    #Check for the response count against the batch count and proceed data processing.
                    raw_files_count = get_raw_downloaded_files(raw_bckt,f"{tmp_formated_batch_run_folder_path}/")
                    logger.info(f"MailIds Batch size - {batch_size} and Actual MailIds response downloaded - {raw_files_count}")
                    if batch_size == raw_files_count:
                        logger.info(f"Success - Recon check on mailids")
                        isDataProcessed, processed_mail_count = process_mail_document_data(
                            tmp_formated_batch_run_folder_path=f"s3://{raw_bckt}/{tmp_formated_batch_run_folder_path}/",
                            relationalized_data_path=f"s3://{raw_bckt}/{relationalized_data_path}/",
                            s3_bckt=raw_bckt,
                            req_extract_schema=schema,
                            renamed_cols=select_renamed_cols,
                            source_name=source_name,
                            function_name=function_name,
                            raw_final_out=parquet_data_path,
                            root_tag=root_name,
                            row_tag=row_tag,
                            sampling_fraction=aconex_mail_document_config["job_parameter"]["sampling_fraction"],
                            sampling_seed=aconex_mail_document_config["job_parameter"]["sampling_seed"],
                            sampling_path=aconex_mail_document_config["job_parameter"]["sampling_output_s3"],
                        )
                        if isDataProcessed:
                            logger.info(f"Batch {batch_number} processed successfully.")
                            logger.info(f"Deleting Batch {batch_number} related temp files.")
                            s3_client.delete_folder_from_s3(temp_path)
                            break
                        else:
                            logger.error(f"Batch {batch_number} processed failed.")
                            s3_client.delete_folder_from_s3(temp_path)
                            sys.exit(1)  # Exit with failure code 
                    else:
                        logger.error(f"Recon check failed for Batch {batch_number} for attempt {batch_attempt}")
                        logger.info(f"Deleting Batch {batch_number} related temp files.")
                        s3_client.delete_folder_from_s3(temp_path)
                        if batch_attempt < 2:
                            batch_attempt=batch_attempt+1
                        else:
                            logger.error(f"Maximum retry attempts reached for Batch {batch_number}. Exiting with error")
                            sys.exit(1)  # Exit with failure code 

    else:
        logger.info(f"No data to process so exiting gracefully")
        job.commit()
        os._exit(0)
    if isDataProcessed:
        print("mail_doc_project_audit_object --> " + audit_full_path)
        audit_date_value = generate_today_date_string(TIMEZONE_UTC, AUDIT_DATE_COLUMN)
        print("audit_date_value --> " + audit_date_value)

        project_audit_column = f"{project_id},{source_name},{function_name},{mail_doc_job_start_time},{audit_date_value}"
        print("project_audit_column --> " + project_audit_column)
        if project_audit_column:
            if s3_client.upload_to_s3(project_audit_column, audit_full_path, kms_key_id, is_gzip=False):
                logger.info(
                    f"Uploaded Aconex mail Document Project info to {raw_bckt}/{incremental_criteria_folder_location}")
                success = True
            else:
                logger.error("Aconex mail Document API was successful but the gzip content could not be uploaded to S3")
                success = False
        else:
            logger.error("Failed to fetch Aconex mail Document API payload")
            success = False
    else:
        logger.error("Failed to fetch Aconex mail Document API payload")
        success = False


    logger.info(f"Delete staging data after relationalizing - {temp_path}")
    logger.info(f"Deleting folder path {temp_path}")
    s3_client.delete_folder_from_s3(temp_path)
    if success:
        job.commit()
    else:    
        sys.exit(1)  # Exit with failure code 
    
except Exception as e: 
    logger.info("Error --> " + str(e))
    # Delete any existing staging data for this project as accumulated staging data can slowdown
    # Relationalize's performance in future runs of the GlueJob
    temp_path=f"s3://{raw_bckt}/{temp_deletion_path}"
    temp_path = s3_client.get_folder_path_from_s3(temp_path)
    logger.info(f"Delete staging data after relationalizing - {temp_path}")
    logger.info(f"Deleting folder path {temp_path}")
    s3_client.delete_folder_from_s3(temp_path)
    sys.exit(1)  # Exit with failure code
