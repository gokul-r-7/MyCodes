from .utilities import utilities 
from .dynamodb import dynamodb
import os
from string import Template
import time
from .metrics import CustomMetrics


custom_metrics = CustomMetrics(service= "rbac-redshift-setup", namespace= 'rbac-framework')
tracer = custom_metrics.get_tracer()


utils = utilities()
ddb = dynamodb()
domain_config = utils.get_tag_config()
settings = utils.get_settings()
db_structure_config = utils.get_database_structure_config()
logger = utils.get_logger()
session = utils.get_aws_session()
redshift_data_client = session.client('redshift-data') 
all_rs_perms_config = utils.get_all_redshift_permissions_config()

account_id = settings["aws_account_id"]

MAX_WAIT_CYCLES = 30
service_roles = settings["service_roles"]

class RedshiftServerlessClient:
    def __init__(self):
        pass
      
    def run_redshift_statement(self,sql_statement,database):
        """
        Generic function to handle redshift statements (DDL, SQL..),
        it retries for the maximum MAX_WAIT_CYCLES.
        Returns the result set if the statement return results.
        """
         

        logger.info(f"Connecting to database:: {database}")
        logger.info("Running statement: {}".format(sql_statement))

        res = redshift_data_client.execute_statement(
            WorkgroupName=settings['redshift_serverless_host'],
            Database=database,
            SecretArn=settings['redshift_serverless_secret_arn'],
            Sql=sql_statement
        )

        # DDL statements such as CREATE TABLE doesn't have result set.
        has_result_set = False
        done = False
        attempts = 0

        while not done and attempts < MAX_WAIT_CYCLES:

            desc = redshift_data_client.describe_statement(Id=res['Id'])
            query_status = desc['Status']

            if query_status == "FAILED":
                if "already exists" in desc["Error"] or "already attached" in desc["Error"]:
                    logger.warning(f"ignoring {desc['Error']}")
                    done = True
                    break
                else:
                    logger.error('SQL query failed: ' + desc["Error"])    
                    raise Exception('SQL query failed: ' + desc["Error"])

            elif query_status == "FINISHED":
                done = True
                has_result_set = desc['HasResultSet']
                break
            else:
                logger.info("Current working... query status is: {} ".format(query_status))

            if not done and attempts >= MAX_WAIT_CYCLES:
                raise Exception('Maximum of ' + str(attempts) + ' attempts reached.')
            
            attempts += 1
            time.sleep(1)

        if has_result_set:
            data = redshift_data_client.get_statement_result(Id=res['Id'])
            response = data['Records']
            return response
    
    def get_target_roles(self):
        distinct_role_names = set()
        for rs_perms_config in all_rs_perms_config:
            for db in rs_perms_config:
                roles = db['ad_roles']
                for role in roles:
                    if role['identity_provider'] == "AWSIDC":
                        role_name = "AWSIDC:" + role['role_name']
                        distinct_role_names.add(role_name)
                    # elif role['identity_provider'] == "AAD":
                    #     role_name = "AAD:" + role['role_name']
                    #     distinct_role_names.add(role_name)
        return distinct_role_names

    def get_current_roles(self):
        database = "dev"
        get_current_roles = f"select role_name from svv_roles where role_owner = 'admin'"
        response = self.run_redshift_statement(get_current_roles, database)
        current_roles = [item[0]['stringValue'] for item in response]
        return current_roles

    def get_new_roles(self):
        target_roles = self.get_target_roles()
        current_roles = set(self.get_current_roles())
        new_roles = list(target_roles - current_roles)
        logger.info(f'Here is a list of AWSIDC roles that need to be created {new_roles}')
        return new_roles
        
    def create_roles(self):
        role_names = self.get_new_roles()
        database = "dev"

        for role_name in role_names:
            logger.info('executing.')
            create_role_ddl = f'CREATE ROLE "{role_name}"'
            self.run_redshift_statement(create_role_ddl,database)
    
    def create_external_schema_dbt(self,external_schema_name,database,glue_db):
        redshift_iam_role=service_roles["redshift_iam_role"]
        create_external_schema_dbt = f"""
                Create external schema if not exists dbt_{external_schema_name}
                from DATA CATALOG database '{glue_db}' 
                catalog_id '{account_id}'
                IAM_ROLE '{redshift_iam_role}';
            """
        self.run_redshift_statement(create_external_schema_dbt, database)

    def create_external_schema_roles(self,external_schema_name,database,glue_db):
        create_external_schema_roles = f"""
                Create external schema if not exists {external_schema_name}
                from DATA CATALOG database '{glue_db}' 
                catalog_id '{account_id}';
            """
        self.run_redshift_statement(create_external_schema_roles, database)
        
    def create_schema(self,database,schema_name):
        create_schema_ddl = f'Create schema if not exists {schema_name}'
        self.run_redshift_statement(create_schema_ddl, database)

    def create_database(self, database):
        create_database_ddl = f'CREATE DATABASE "{database}"'
        self.run_redshift_statement(create_database_ddl, "dev")

    def grant_create_schema_to_user(self,schema_name,dbt_user,database):
        grate_create_ddl = f'grant create on schema {database}.{schema_name} to {dbt_user}'
        self.run_redshift_statement(grate_create_ddl, database)
    
    def grant_usage_schema(self,schema_name,entity_name,database,identity_provider):
        if identity_provider == "AWSIDC" or "AAD":
            grant_usage_ddl = f'grant usage on schema {schema_name} to role "{identity_provider}:{entity_name}"'
        if identity_provider == "internal":
            grant_usage_ddl = f'grant usage on schema {schema_name} to {entity_name}'
        
        self.run_redshift_statement(grant_usage_ddl, database)

    def grant_select_on_all_tables_schema(self,schema_name,entity_name,database,identity_provider):
        if identity_provider == "AWSIDC" or "AAD":
            grant_usage_ddl = f'grant select on all tables in schema {schema_name} to role "{identity_provider}:{entity_name}"'
        if identity_provider == "internal":
            grant_usage_ddl = f'grant select on all tables in schema {schema_name} to {entity_name}'
        
        self.run_redshift_statement(grant_usage_ddl, database)
        
    def read_sql_script(self,file_path):
        with open(file_path, "r") as sql_file:
            sql_script = sql_file.read().strip()
            statements = sql_script.split(';')
            statements = [statement.strip() for statement in statements if statement.strip()]
        return statements

    def create_rls_policy(self,tgt_db,schema):
        try:
            sql_file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f"../policies/{tgt_db}/{schema}/rls_create.sql"
            )
            statements = self.read_sql_script(sql_file_path)
            for statement in statements:
                template = Template(statement)
                # TODO - change sql file domain to schema
                statement = template.substitute(domain=schema)
                self.run_redshift_statement(statement,tgt_db)
            logger.info("Created rls policy")
    
        except FileNotFoundError:
            logger.warning(f"File - {sql_file_path} - not found, please add file")   
            raise    
    
    def attach_rls_policy(self, tgt_db, schema, obt_name):
        roles = self.get_roles_for_rls(tgt_db)
        for role in roles:
            attach_rls_ddl = f"""
                ATTACH RLS POLICY {schema}_rls_policy 
                ON {schema}.{obt_name}
                TO {role};
            """
            self.run_redshift_statement(attach_rls_ddl,tgt_db)
        logger.info("Attached rls policy")

        alter_table_rls = f"""
            ALTER table {schema}.{obt_name} ROW LEVEL security on
        """
        self.run_redshift_statement(alter_table_rls,tgt_db)
        logger.info("Turn on RLS security for table")

    def get_roles_for_rls(self, tgt_db):
        perms_config = utils.get_redshift_permissions_config(db=tgt_db)
        for db in perms_config:
            roles = db['ad_roles']
            role_list = []
            for role in roles:
                role_name = role['role_name']
                identity_provider = role['identity_provider']
                if identity_provider == "AAD":
                    role_list.append(f"ROLE \"AAD:{role_name}\"")
                if identity_provider == "AWSIDC":
                    role_list.append(f"ROLE \"AWSIDC:{role_name}\"")
        return role_list

    def create_domain_auth_lookup_table(self,schema,database):
        table_name = f"{schema}_security"
        create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                Project_ID varchar(100) NOT NULL ENCODE lzo,
                User_Name varchar(200) NOT NULL ENCODE lzo
            ) DISTSTYLE AUTO;
        """

        self.run_redshift_statement(create_table_ddl,database)
        logger.info('Table created successfully.')

    def import_s3_file(self,database,bucket,key,schema,region):
        """
            Loads the content of the S3 temporary file into the Redshift table.
        """
        table_name = f"{schema}_security"
        logger.info(f"loading data to {table_name} table in {database}.{schema}"
                    f" please ensure the table is created via dbt")

        load_data_ddl = f"""
            COPY {schema}.{table_name} 
            FROM 's3://{bucket}/{key}' 
            DELIMITER ','
            IGNOREHEADER as 1
            REGION '{region}'
            IAM_ROLE default;
        """

        self.run_redshift_statement(load_data_ddl,database)
        logger.info('Imported S3 file to Redshift.')
    
    def revoke_usage_schema_role(self, schema_name, entity_name, identity_provider, database):
        if identity_provider == "AWSIDC" or "AAD":
            revoke_usage_ddl = f'revoke usage on schema {schema_name} from role "{identity_provider}:{entity_name}"'
        if identity_provider == "internal":
            revoke_usage_ddl = f'revoke usage on schema {schema_name} from {entity_name}'
        self.run_redshift_statement(revoke_usage_ddl, database)
    
    def revoke_select_tables_role(self, schema_name, entity_name, identity_provider, database):
        if identity_provider == "AWSIDC" or "AAD":
            revoke_select_ddl = f'revoke select on all tables in schema {schema_name} from role "{identity_provider}:{entity_name}"'
        if identity_provider == "internal":
            revoke_select_ddl = f'revoke select on all tables in schema {schema_name} from {entity_name}'
        self.run_redshift_statement(revoke_select_ddl, database)


        