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
logger = utils.get_logger()
session = utils.get_aws_session()
redshift_data_client = session.client('redshift-data') 


MAX_WAIT_CYCLES = 30
service_roles = settings["service_roles"]

class redshiftClient:
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
            ClusterIdentifier=settings['redshift_host'],
            Database=database,
            DbUser='admin',
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

            if has_result_set:
                data = redshift_data_client.get_statement_result(Id=res['Id'])
                return data
            
            attempts += 1
            time.sleep(1)    

    def create_domain_auth_lookup_table(self,schema):
        table_name = f"{schema}_security"
        create_table_ddl = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table_name} (
                Project_ID varchar(100) NOT NULL ENCODE lzo,
                User_Name varchar(200) NOT NULL ENCODE lzo
            ) DISTSTYLE AUTO;
        """

        self.run_redshift_statement(create_table_ddl,"global_standard_reporting")
        logger.info('Table created successfully.')

    def import_s3_file(self,database,bucket,key,schema,region):
        """
            Loads the content of the S3 temporary file into the Redshift table.
        """
        table_name = f"{schema}_security"

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

    def get_distinct_role_names(self):
        distinct_role_names = set()
        
        for domain in domain_config['domain']:
            logger.info(f"Getting roles for {domain['name']} domain")
            for role in domain['roles']:
                if 'role_name' in role:
                    role_name = "AWSIDC:" + role['role_name']
                    logger.info(f"Adding {role['role_name']} for {domain['name']} domain to list of roles")
                    distinct_role_names.add(role_name)
        
        for data_classification in domain_config['data_classification']:
            logger.info(f"Getting roles for {data_classification['name']} group")
            for role in data_classification['roles']:
                if 'role_name' in role:
                    role_name = "AWSIDC:" + role['role_name']
                    logger.info(f"Adding {role['role_name']} for {data_classification['name']} group to list of roles")
                    distinct_role_names.add(role_name)
        
        return list(distinct_role_names)
    
    def get_domain_names(self):
        domains = domain_config['domain']
        domain_names = [domain.get("name").lower() for domain in domains]
        return domain_names
    
    def create_roles(self):
        role_names = self.get_distinct_role_names()
        database = "dev"

        for role_name in role_names:
            logger.info('executing.')
            create_role_ddl = f'CREATE ROLE "{role_name}"'
            self.run_redshift_statement(create_role_ddl,database)


    def create_domain_databases(self,domains,database):

        for i in domains:
            create_database_ddl = f'CREATE DATABASE "{i}"'
            self.run_redshift_statement(create_database_ddl, database)
            
    def get_consumer_databases(self):
        consumer_databases = settings["consumer_databases"]
        filtered_consumer_databases= []
        for db in consumer_databases:
            if db == "global_standard_reporting":
                logger.warning("Getting a list of consumer databases, skipping global_standard_reporting")
                continue
            else:
                filtered_consumer_databases.append(db)
        return filtered_consumer_databases
                   
    def set_up_domain_db(self):
        """
        for each domain in the governance_config.json, it creates:
            a domain database
            
            for each glue database for that domain, it creates:
                an external schema for dbt - with IAM role attached
                an external schema for human roles - without IAM role 
                grant usage on dbt external schema to dbt user
                grant select on all tables in dbt external schema to dbt user
            
            a domain_integrated_model schema
            grant domain_integrated_model schema usage to dbt user
            grant create on schema domain_integrated_model to dbt user
        """
        account_id = settings["aws_account_id"]
        domain_names = self.get_domain_names()
        
        filtered_consumer_databases = self.get_consumer_databases()
        dbt_consumer_users = [f"dbt_{db}_user" for db in filtered_consumer_databases]
        
        self.create_domain_databases(domain_names, "dev")

        for domain in domain_names:
            lake_database_names = ddb.get_database_names_by_domain(domain)
            dbt_user = f"dbt_{domain}_user"
            all_dbt_users = [dbt_user] + dbt_consumer_users
            
            for lake_database_name in lake_database_names:
                source_name = ddb.get_source_name_by_database(lake_database_name)
                dbt_schema_name = f'dbt_curated_{source_name}'
                self.create_external_schema(
                    source_name=source_name,
                    database=domain,
                    account_id=account_id,
                    lake_database=lake_database_name
                )

                for user in all_dbt_users:
                    self.grant_usage_schema_user(
                        schema_name=dbt_schema_name,
                        dbt_user=user,
                        database=domain
                    )
                    
                    self.grant_select_on_all_tables_schema_user(
                        schema_name=dbt_schema_name,
                        dbt_user=user,
                        database=domain
                    )

            for i in settings["consumer_schemas"]:
                schema_name = i
                self.create_schema(database=domain,schema_name=schema_name)
                self.grant_usage_schema_user(
                    schema_name=schema_name,
                    dbt_user=dbt_user,
                    database=domain
                )
                self.grant_create_schema_to_user(
                    domain=domain,
                    schema_name=schema_name,
                    database=domain,
                    dbt_user=dbt_user
                )
            
            for db in filtered_consumer_databases:

                schema_name = f"{db}_model"
                dbt_user = f"dbt_{db}_user"
                self.create_schema(database=domain,schema_name=schema_name)
                self.grant_usage_schema_user(
                    schema_name=schema_name,
                    dbt_user=dbt_user,
                    database=domain
                )
                self.grant_create_schema_to_user(
                    domain=domain,
                    schema_name=schema_name,
                    database=domain,
                    dbt_user=dbt_user
                )
                self.grant_select_on_all_tables_schema_user(
                    schema_name=schema_name,
                    dbt_user=dbt_user,
                    database=domain
                )

    def create_external_schema(self,source_name,database,account_id,lake_database,create_dbt_schema=True,create_consumption_schema=True):
            #redshift_iam_role=settings["redshift_iam_role"]
            redshift_iam_role=service_roles["redshift_iam_role"]
            
            if create_dbt_schema:
                create_external_schema_for_dbt_ddl = f"""
                        Create external schema if not exists dbt_curated_{source_name} 
                        from DATA CATALOG database '{lake_database}' 
                        catalog_id '{account_id}' 
                        IAM_ROLE '{redshift_iam_role}';
                    """
                self.run_redshift_statement(create_external_schema_for_dbt_ddl, database)

            if create_consumption_schema:
                create_external_schema_for_humans_ddl = f"""
                        Create external schema if not exists curated_{source_name} 
                        from DATA CATALOG database '{lake_database}' 
                        catalog_id '{account_id}';
                    """
                self.run_redshift_statement(create_external_schema_for_humans_ddl, database)
    
    def grant_usage_schema_user(self,schema_name,dbt_user,database):
        grant_schema_ddl = f'Grant usage on schema {schema_name} to {dbt_user}'
        self.run_redshift_statement(grant_schema_ddl,database)

    def grant_select_on_all_tables_schema_user(self,schema_name,dbt_user,database):
        grant_select_ddl = f'Grant select on all tables in schema {schema_name} to {dbt_user}'
        self.run_redshift_statement(grant_select_ddl, database)

    def create_schema(self,database,schema_name):
        create_schema_ddl = f'Create schema if not exists {schema_name}'
        self.run_redshift_statement(create_schema_ddl, database)

    def create_database(self, database):
        create_database_ddl = f'CREATE DATABASE "{database}"'
        self.run_redshift_statement(create_database_ddl, "dev")

    def grant_usage_database_user(self,domain,dbt_user,database):
        grant_usage_ddl = f'grant usage for schemas in database {domain} to {dbt_user}'
        self.run_redshift_statement(grant_usage_ddl, database)

    def grant_create_schema_to_user(self,domain,schema_name,dbt_user,database):
        grate_create_ddl = f'grant create on schema {domain}.{schema_name} to {dbt_user}'
        self.run_redshift_statement(grate_create_ddl, database)

    def grant_usage_schema_role(self,schema_name,role_name,database):
        grant_usage_ddl = f'grant usage on schema {schema_name} to role "{role_name}"'
        self.run_redshift_statement(grant_usage_ddl, database)

    def grant_select_tables_role(self,schema_name,role_name,database):
        grant_select_ddl = f'grant select on all tables in schema {schema_name} to role "{role_name}"'
        self.run_redshift_statement(grant_select_ddl, database)

    def grant_usage_schema_user_name(self,schema_name,user_name,database):
        grant_usage_ddl = f'grant usage on schema {schema_name} to "{user_name}"'
        self.run_redshift_statement(grant_usage_ddl, database)

    def grant_select_tables_user_name(self,schema_name,user_name,database):
        grant_select_ddl = f'grant select on all tables in schema {schema_name} to "{user_name}"'
        self.run_redshift_statement(grant_select_ddl, database)


    def set_up_consumer_db(self,database):
        """
        connect to dev database:
            create a gsr database
        connect to gsr database:
            grant gsr database usage to dbt_gsr_user
            for each domain in governance_config.json:
                create schema(domain)
                grant create on schema(domain) of gsr database to dbt_gsr_user
        for each domain in governance_config.json:
            connect to domain database:
                grant domain_integrated_model schema usage to dbt_gsr_user
                grant select on tables in domain_integrated_model schema to dbt_gsr_user
        """
        dbt_user = f"dbt_{database}_user"
        self.create_database(database)
        
        domain_names = self.get_domain_names()
        
        self.grant_usage_database_user(domain=database, dbt_user=dbt_user, database=database)
        for domain in domain_names:
            self.create_schema(database=database,schema_name=domain)
            self.grant_create_schema_to_user(domain=database,schema_name=domain,database=database,dbt_user=dbt_user)

            schema_name = "domain_integrated_model"
            self.grant_usage_schema_user(schema_name=schema_name,dbt_user=dbt_user,database=domain)
            self.grant_select_on_all_tables_schema_user(schema_name=schema_name,dbt_user=dbt_user,database=domain)               
    
    @tracer.capture_method
    def grant_permissions_roles(self,tgt_domain):
        """
        for each domain in governance_config.json, go through roles:
            connect to its domain database:
                get all glue databases for that domain, which has corresponding external schemas in redshift
                    for each external schema for human roles:
                        grant usage on schema to role
                        grant select on all tables in schema to role
                grant usage on domain_integrated_model to role
                grant select on all tables in domain_integrated_model to role
        """

        def grant_schema_select_permissions(database_name,role,schema_name):
            # grant roles permissions to domain_integrated_model schema
            if 'role_name' in role:
                self.grant_usage_schema_role(
                    schema_name=schema_name,
                    role_name=f"AWSIDC:{role['role_name']}",
                    database=database_name
                )

                self.grant_select_tables_role(
                    schema_name=schema_name,
                    role_name=f"AWSIDC:{role['role_name']}",
                    database=database_name
                )
            else:
                logger.info("Entered in user grants")
                self.grant_usage_schema_user_name(
                    schema_name=schema_name,
                    user_name=role['user_name'],
                    database=database_name
                )
                self.grant_select_tables_user_name(
                    schema_name=schema_name,
                    user_name=role['user_name'],
                    database=database_name
                )


        def handle_role_grants(domain, role):

            allowed_objects = role.get('allowed_objects', [])


            for obj in allowed_objects:
                database_name = obj['database_name']
                schema_name = obj['schema']

                # note - the domain permission grants will be used in a lambda function invoked by the gsr dag
                if database_name == domain['name'].lower():
                    if schema_name == '*':

                        # get all glue databases for that domain 
                        databases = ddb.get_database_names_by_domain(domain['name'].lower())
                        # for each glue database, it has an external schema for human roles in the redshift database, apply grants to roles
                        for database in databases:
                            source_name = ddb.get_source_name_by_database(database)
                            schema_for_roles = f'curated_{source_name}'
                            grant_schema_select_permissions(database_name,role,schema_for_roles)

                        grant_schema_select_permissions(database_name, role, "domain_integrated_model")
                    else:
                        grant_schema_select_permissions(database_name, role,schema_name)


                else:
                    grant_schema_select_permissions(database_name,role,schema_name)


        # Iterate over the domains and roles
        
        for domain in domain_config['domain']:
            if tgt_domain.lower() in domain['name'].lower():
                for role in domain['roles']:
                    handle_role_grants(domain, role)

    @tracer.capture_method
    def grant_permissions_users(self,tgt_domain):
        """
            grant user permission to schema on the domain/database
            grant usage on domain_integrated_model/snoflake_model to users
            grant select on all tables in domain_integrated_model/snowflake_model to users
        """

        def grant_schema_select_permissions(database_name,user_name,schema_names):
            # grant users permissions to domain_integrated_model schema
            for schema_name in schema_names:
                self.grant_usage_schema_user_name(
                        schema_name=schema_name,
                        user_name=user_name,
                        database=database_name
                    )
                self.grant_select_tables_user_name(
                    schema_name=schema_name,
                    user_name=user_name,
                    database=database_name
                )
            
        # Iterate over the domains
        for domain in domain_config['domain']:
            if tgt_domain.lower() in domain['name'].lower():
                """
                    The user name and schema need to be derived from config file.
                    The congif file need allingment to support the user base acces to schemas
                """
                # grant_schema_select_permissions(tgt_domain.lower(),'snow_user',['snowflake_model','domain_integrated_model'])
                grant_schema_select_permissions(tgt_domain.lower(),'dbt_global_standard_reporting_user',['domain_integrated_model'])

    def read_sql_script(self,file_path):
        with open(file_path, "r") as sql_file:
            sql_script = sql_file.read().strip()
            statements = sql_script.split(';')
            statements = [statement.strip() for statement in statements if statement.strip()]
        return statements

    def create_rls_policy(self,tgt_db,domain):

        try:
            sql_file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f"../policies/{tgt_db}/{domain}/rls_create.sql"
            )
            statements = self.read_sql_script(sql_file_path)
    
            for statement in statements:
                template = Template(statement)
                statement = template.substitute(domain=domain)
                self.run_redshift_statement(statement, tgt_db)
            logger.info("Created rls policy")
        except Exception as e:
            logger.error(f"ERROR - {e}")
    
    def attach_rls_policy(self,tgt_db,domain,obt_name):
        try:
            sql_file_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                f"../policies/{tgt_db}/{domain}/rls_attach.sql"
            )
            statements = self.read_sql_script(sql_file_path)
    
            for statement in statements:
                template = Template(statement)
                statement = template.substitute(domain=domain,
                                                obt_name=obt_name, 
                                                grant_statement=self.get_rls_attach_grant_statement(domain))
                logger.info(f"executing ::: {statement}")
                self.run_redshift_statement(statement, tgt_db)
            logger.info("Attached rls policy")
        except Exception as e:
            logger.error(f"ERROR - {e}")


    def get_rls_attach_grant_statement(self,domain):
        for i in domain_config['domain']:
            if i['name'] == domain.title():
                logger.info(f"Attaching RLS policy to domain :{domain}")
                role_list = []
                for x in i['roles']:
                    if 'role_name' in x:
                        role_list.append(f"ROLE \"AWSIDC:{x['role_name']}\"")
                                     
        x = ", ".join(role_list)
        return x

    def is_revoke_role(self,role):
        try:
            if role["active"] == False:
                return True
        
            return False
        
        except KeyError:
            return False
        
    def revoke_usage_schema_role(self, schema_name, role_name, database):
        revoke_usage_ddl = f'revoke usage on schema {schema_name} from role "{role_name}"'
        self.run_redshift_statement(revoke_usage_ddl, database)
    
    def revoke_select_tables_role(self, schema_name, role_name, database):
        revoke_select_ddl = f'revoke select on all tables in schema {schema_name} from role "{role_name}"'
        self.run_redshift_statement(revoke_select_ddl, database)

    def revoke_permissions(self):

        for domain in domain_config['domain']:
            for role in domain['roles']:
                logger.debug(role)
                if self.is_revoke_role(role):
                    logger.info(f"Revoking permissions to {role['role_name']}")
                    role_name = "AWSIDC:" + role['role_name']
                    allowed_objects = role.get('allowed_objects', [])

                    for obj in allowed_objects:
                        database_name = obj['database_name']
                        schema_name = obj['schema']

                        # note - the domain permission grants will be used in a lambda function invoked by the gsr dag
                        if database_name == domain['name'].lower():
                            if schema_name == '*':

                                # get all glue databases for that domain
                                databases = ddb.get_database_names_by_domain(domain['name'].lower())
                                # for each glue database, it has an external schema for human roles in the redshift database, apply grants to roles
                                for database in databases:
                                    source_name = ddb.get_source_name_by_database(database)
                                    schema_for_roles = f'curated_{source_name}'
                                    self.revoke_usage_schema_role(
                                            schema_name=schema_for_roles,
                                            role_name=role_name,
                                            database=database_name
                                    )

                                    self.revoke_select_tables_role(
                                        schema_name=schema_for_roles,
                                        role_name=role_name,
                                        database=database_name
                                    )

                                # grant roles permissions to domain_integrated_model schema
                                domain_integrated_schema = "domain_integrated_model"
                                self.revoke_usage_schema_role(
                                            schema_name=domain_integrated_schema,
                                            role_name=role_name,
                                            database=database_name
                                )

                                self.revoke_select_tables_role(
                                        schema_name=domain_integrated_schema,
                                        role_name=role_name,
                                        database=database_name
                                )
                            
                            else:
                                self.revoke_usage_schema_role(
                                    schema_name=schema_name,
                                    role_name=role_name,
                                    database=database_name
                                )
                                
                                self.revoke_select_tables_role(
                                    schema_name=schema_name,
                                    role_name=role_name,
                                    database=database_name
                                )

                        # note - the global_standard_reporting permission grants will be used in a lambda function invoked by the gsr dag
                        else:
                            self.revoke_usage_schema_role(
                                            schema_name=schema_name,
                                            role_name=role_name,
                                            database=database_name
                            )

                            self.revoke_select_tables_role(
                                    schema_name=schema_name,
                                    role_name=role_name,
                                    database=database_name
                            )




if __name__ == "__main__":
    # redshift = Redshift()
    rsc = redshiftClient()
    rsc.create_domain_auth_lookup_table("engineering")
    ##redshift.create_roles()
    ##redshift.set_up_domain_db()
    ##redshift.set_up_gsr_db()
    ##redshift.grant_permissions_roles()
    #redshift.create_rls_policy()
    #redshift.attach_rls_policy()