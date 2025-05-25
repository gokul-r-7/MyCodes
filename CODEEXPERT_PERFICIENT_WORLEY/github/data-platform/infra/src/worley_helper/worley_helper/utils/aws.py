import boto3
import logging
import json
from datetime import datetime
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeSerializer
from worley_helper.utils.logger import get_logger
from worley_helper.utils.constants import REGION
from dynamodb_json import json_util
from typing import Any

logger = get_logger(__name__)


class DynamoDB:
    def __init__(self, metadata_table_name: str, default_region: str = REGION):
        self.dynamo_resource = boto3.resource("dynamodb", region_name=default_region)
        self.table = self.dynamo_resource.Table(metadata_table_name)
        self.metadata_table_name = metadata_table_name

    def get_metadata_from_ddb(self, source_system_id: str, metadata_type: str) -> dict:
        """
        Gets a Metadata Item from the relevant Table

        Parameters
        ----------
        Args:
            source_system_id: str:
                The name of the Source System ID i.e. oracle_p6
            metadata_type: str:
                The value for the metadata type

        Returns
        -------
            A dictionary with the DynamoDB Item

        Examples
        -------
        >>> ddb = DynamoDB(metadata_table_name="worley-dev-metadatatable", default_region="ap-southeast-2")
        >>> metadata = ddb.get_metadata_from_ddb(source_system_id="ecosys_curated", metadata_type="curated#ecosys#calendar#job#iceberg")
        """
        try:
            response = self.table.query(
                KeyConditionExpression=Key("SourceSystemId").eq(source_system_id)
                & Key("MetadataType").eq(metadata_type)
            )
            return json_util.loads(response["Items"][0])
        except ClientError as e:
            logger.error(e)
            raise e

    def get_masking_metadata_from_ddb(self, databasename: str) -> dict:
        """
        Gets a Metadata Item from the relevant Table

        Parameters
        ----------
        Args:
            databasename: str:
                The name of the database

        Returns
        -------
            A dictionary with the DynamoDB Item

        Examples
        -------
        >>> ddb = DynamoDB(metadata_table_name="worley-mf-sydney-dev-database-permissions-metadata", default_region="ap-southeast-2")
        >>> metadata = ddb.get_masking_metadata_from_ddb(database_name="worley_datalake_sydney_dev_glue_catalog_database_construction_o3_curated")
        """
        try:
            response = self.table.query(
                KeyConditionExpression=Key("database_name").eq(databasename)
            )
            return json_util.loads(response["Items"][0])
        except ClientError as e:
            logger.error(e)
            raise e

    def get_project_metdata_from_ddb(self, domain_name: str, target_table:str) -> list:
        """
        Gets a Metadata Item from the relevant Table

        Parameters
        ----------
        Args:
            source_system_id: str:
                The name of the Source System ID i.e. oracle_p6
            metadata_type: str:
                The value for the metadata type

        Returns
        -------
            A dictionary with the DynamoDB Item
        """
        
        try:
            response = self.table.query(
                KeyConditionExpression=Key("domain").eq(domain_name)
                & Key("target_table").eq(target_table)
            )
            return json_util.loads(response["Items"])
        except ClientError as e:
            logger.error(e)
            raise e


    @classmethod
    def convert_dict_to_dynamodb(
        self,
        item: dict,
    ) -> dict:
        """Function that converts a Python Dictionary to a DynamoDB JSON item"""
        serializer = TypeSerializer()
        return {key: serializer.serialize(value) for key, value in item.items()}

class S3:
    def __init__(
        self, bucket_name : str, default_region: str = REGION
    ):
        self.s3_client = boto3.client("s3")
        self.bucket  = bucket_name

    def s3_folder_exists(self,folder_path):
        """
        Check if folder exists in s3
        :param bucket:
        :param folder_path:
        :return:
        """

        if not folder_path.endswith("/"):
            folder_path += "/"

        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket, Prefix=folder_path, Delimiter="/"
        )

        return "Contents" in response or "CommonPrefixes" in response


    def read_s3_file_as_json(self, folder_path: str, file_name: str = "") -> dict:
        """
        Read JSON from an S3 bucket.

        :param folder_path: Folder path within the bucket (str).
        :param file_name: (Optional) Name of the JSON file (str). Defaults to empty string.
        :return: Data loaded from the JSON file as a dictionary.
        """
        try:
            logger.info(f"Reading JSON file from S3 folder: {folder_path}")
            logger.info(f"Reading JSON file from S3 file: {file_name}")
            # Construct s3_path based on whether file_name is provided
            if file_name:
                s3_path = f"{folder_path}/{file_name}"
            else:
                s3_path = folder_path

            logger.info(f"Reading JSON file from S3 path: {s3_path}")

            response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_path)
            data = response["Body"].read().decode("utf-8")
            result = json.loads(data)
            return result  # Returning the JSON data as a dictionary
        except Exception as e:
            # Log and handle exceptions
            logger.error(f"Error reading JSON file from S3: {e}")
            return {}  # Returning an empty dictionary if an error occurs


    def read_s3_folder_as_json(self,folder_path: str) -> list:
        """
        Read json from s3
        :param bucket: Bucket
        :param folder_path: folder path
        :return: data as dict
        """
        bucket_name = self.bucket

        # Initialize an empty list to store DataFrames
        data_array = []

        if self.s3_folder_exists(self.bucket, folder_path):
            print(f"Loading objects in {bucket_name}/{folder_path}")
            # Get a list of all objects in the folder
            s3_objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_path)

            # Appends progressively to output
            for obj in s3_objects["Contents"]:
                file_name = obj["Key"]
                file_name = file_name.split("/")[-1]

                data = self.read_s3_file_as_json(self.bucket, folder_path, file_name)
                data_array.append(data)
            print(f"Finished loading objects in {bucket_name}/{folder_path}")
        else:
            print(f"No objects in {bucket_name}/{folder_path}")

        return data_array


    def get_fields_from_s3(self,folder_path: str, field_list: list):
        json_data = self.read_s3_folder_as_json(self.bucket, folder_path)

        return_array = []
        for data in json_data:

            if "data" in data:
                if isinstance(data.get("data"), list):
                    data = data.get("data")
                else:
                    data = [data.get("data")]
            elif type(data) is list:
                pass
            else:
                data = []

            for record in data:
                return_record = {}
                for field_id in field_list:
                    return_record[field_id] = record.get(field_id)
                return_array.append(return_record)

        # assume first item in list is the key, use that to filter for duplicates
        return list({item[field_list[0]]: item for item in return_array}.values())

    def delete_file_from_s3(self, file_name: str):
        """
        Deletes a file from an S3 bucket.
        :param file_name: Name of the file to delete (str).
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket, Key=file_name)
            logger.info(f"Successfully deleted file {file_name} from S3.")
        except ClientError as e:
            logger.error(f"Error deleting file {file_name} from S3: {e}")


    def delete_folder_from_s3(self, folder_path: str):
        """
        Deletes all objects in the specified folder path from an S3 bucket.

        Args:
            folder_path (str): The path of the folder to be deleted, without the bucket name.
                              Example: "path/to/folder"
        """
        try:
            # List all objects in the folder
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=folder_path)

            # Delete the objects
            while response['KeyCount'] > 0:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                self.s3_client.delete_objects(Bucket=self.bucket, Delete={'Objects': objects_to_delete})
                try:
                    response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=folder_path, ContinuationToken=response['NextContinuationToken'])
                except KeyError:
                    # No more objects to delete
                    break

            logger.info(f"Successfully deleted all objects in {self.bucket}/{folder_path}")

        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                logger.error(f"Error: Bucket {self.bucket} does not exist.")
            else:
                logger.error(f"Error deleting objects in {self.bucket}/{folder_path}: {e}")
        except Exception as e:
            logger.error(f"Error deleting objects in {self.bucket}/{folder_path}: {e}")


    def get_folder_path_from_s3(self, s3_path: str) -> str:
        """
        Extracts the folder path from an S3 path, excluding the bucket name.

        Args:
            s3_path (str): The full S3 path, including the bucket name.
                        Example: "s3://my-bucket/path/to/folder"

        Returns:
            str: The folder path, excluding the bucket name.
                Example: "path/to/folder"
        """
        if s3_path.startswith("s3://"):
            s3_path = s3_path[5:]  # Remove the "s3://" prefix
        bucket_name, *folder_path = s3_path.split("/", 1)
        if folder_path:
            return "/".join(folder_path)
        else:
            return ""


    def upload_to_s3(
        self,
        content: bytes,
        key: str,
        kms_key_id: str,
        is_gzip: bool = False
    ) -> bool:
        """
        Uploads content to an S3 bucket
        :param content: Content to upload (bytes)
        :param key: Key of the object in the S3 bucket (str)
        :param kms_key_id: ID of the KMS key used for encryption (str)
        :param is_gzip: Indicates whether the content is gzip compressed (bool)
        :return: True if upload was successful, False otherwise (bool)
        """
        try:
            extra_args = {}
            if is_gzip:
                extra_args["ContentEncoding"] = "gzip"
            # This will be enabled later
            extra_args["ServerSideEncryption"] = "aws:kms"
            extra_args["SSEKMSKeyId"] = kms_key_id

            self.s3_client.put_object(
                Body=content,
                Bucket=self.bucket,
                Key=key,
                **extra_args
            )

            logger.info(
                f"Successfully uploaded content to s3://{self.bucket}/{key}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error uploading content to s3://{self.bucket}/{key}, {e}"
            )
            return False

    def read_s3_file(self, folder_path: str, file_name: str) -> dict:
        """
        Read file from an S3 bucket.

        :param folder_path: Folder path within the bucket (str).
        :param file_name: (Required) Name of the file (str).
        :return: Data loaded
        """
        try:
            logger.info(f"Reading file from S3 folder: {folder_path}")
            logger.info(f"Reading file from S3 file: {file_name}")
            # Construct s3_path based on whether file_name is provided
            if file_name:
                s3_path = f"{folder_path}/{file_name}"
            else:
                s3_path = folder_path

            response = self.s3_client.get_object(
                Bucket=self.bucket, Key=s3_path)

            data = response["Body"].read().decode("utf-8")

            return data.strip(), True
        except Exception as e:
            # Log and handle exceptions
            logger.error(f"Error reading file from S3: {e}")
            return None, False




class SecretsManager:

    def __init__(
        self, default_region: str ):
        self.ssmsession = boto3.session.Session()
        self.ssmclient = self.ssmsession.client(service_name="secretsmanager",region_name=default_region)


    def get_secret(self,secret_name):
        """Get secret from AWS Secrets Manager"""
        print(f"Getting secret {secret_name}")
        print(f"Region: {REGION}")

        try:
            get_secret_value_response = self.ssmclient.get_secret_value(SecretId=secret_name)
        except ClientError as exp:
            raise exp
        secret = get_secret_value_response["SecretString"]
        return secret

def get_parameter(name: Any, with_decryption=True):
    """Get parameter from AWS SSM"""
    try:
        aws_session = boto3.Session(region_name=REGION)
        parameter = aws_session.client("ssm").get_parameter(Name=name, WithDecryption=with_decryption)
    except Exception as exp:
        raise exp
    return parameter["Parameter"]["Value"]


def get_secret(secret_name: str, region_name: str) -> str:
    """
    Get secret from AWS Secrets Manager.
    Args:
        secret_name (str): The name of the secret to be retrieved.
        region_name (str): The AWS region where the secret is stored.
    Returns:
        str: The value of the retrieved secret.
    Raises:
        ClientError: If an error occurs while retrieving the secret.
    """
    logger.info(f"Getting secret {secret_name}")
    logger.info(f"Region: {region_name}")
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as exp:
        raise exp
    secret = get_secret_value_response["SecretString"]

    return secret

def get_aws_account_id() -> str:
    """
    Get the AWS account ID for the current AWS credentials.
    Returns:
        str: The AWS account ID.
    Raises:
        Exception: If an error occurs while retrieving the account ID.
    """
    logger.info("Getting AWS account ID")
    sts_client = boto3.client(service_name="sts")
    
    try:
        response = sts_client.get_caller_identity()
        account_id = response["Account"]
    except Exception as exp:
        logger.error(f"Error getting AWS account ID: {exp}")
        raise exp
    
    logger.info(f"AWS account ID: {account_id}")
    
    return account_id

