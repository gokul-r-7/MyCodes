# oracle_p6_customer_dags.py
# import os
# import logging
# from typing import Dict
#from oracle_p6_dag_factory import OracleP6CustomerDagFactory
import logging
import os
import glob
import yaml
import boto3
import datetime
from typing import Dict, List, Optional
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.operators.bash import BashOperator
from airflow.decorators import task_group
from airflow.datasets import Dataset
from boto3.dynamodb.conditions import Key

logger = logging.getLogger(__name__)

class CustomerConfig:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
            self.customer_id = self.config['customer']['id']
            self.environments = self.config['customer']['environments']

    def get_dag_defaults(self) -> Dict:
        return self.config['dag_defaults']

    def get_aws_config(self) -> Dict:
        return self.config['aws']

    def get_glue_jobs(self) -> Dict:
        return self.config['glue_jobs']

class OracleP6CustomerDagFactory:
    def __init__(self, config_dir: str):
        self.config_dir = config_dir
        self.customer_configs = self._load_customer_configs()

    def _load_customer_configs(self) -> Dict[str, CustomerConfig]:
        configs = {}
        config_paths = glob.glob(os.path.join(self.config_dir, "**/*.yml"), recursive=True)
        
        for config_path in config_paths:
            customer_config = CustomerConfig(config_path)
            configs[customer_config.customer_id] = customer_config
        
        return configs

    def _get_metadata_from_ddb(self, customer_id: str, environment: str) -> dict:
        """Fetch metadata from DynamoDB for specific customer and environment"""
        config = self.customer_configs[customer_id].config
        aws_config = config['aws']
        
        dynamo_resource = boto3.resource(
            "dynamodb",
            region_name=aws_config['region']
        )
        
        table_name = aws_config['dynamodb']['metadata_table'].format(env=environment)
        source_system_id = aws_config['dynamodb']['source_system_id']
        input_keys = f"api#{source_system_id}#airflowRunConfig"
        
        table = dynamo_resource.Table(table_name)
        
        try:
            response = table.query(
                KeyConditionExpression=Key("SourceSystemId").eq(source_system_id) & 
                                    Key("MetadataType").eq(input_keys)
            )
            return response["Items"][0] if response["Items"] else {}
        except Exception as e:
            print(f"Error fetching metadata for {customer_id}-{environment}: {e}")
            return {}

    def create_task_groups(self, dag: DAG, customer_id: str, environment: str, metadata: Dict):
        """Create all task groups for the DAG"""
        config = self.customer_configs[customer_id].config
        aws_config = config['aws']
        
        @task_group(group_id=config['task_groups']['global_sourcing']['id'])
        def oracle_ps_global_sourcing():
            for function_name in metadata[environment][f'Project_{config["customer"]["id"]}']['data_type']['global']:
                GlueJobOperator(
                    task_id=f"oracle_p6_sourcing_api_global_{function_name}",
                    job_name=config['glue_jobs']['patterns']['sourcing_api'].format(
                        env=environment
                    ),
                    region_name=metadata[environment]['region'],
                    script_args={
                        "--source_name": aws_config['dynamodb']['source_system_id'],
                        "--function_name": function_name,
                        "--project_id": "0",
                        "--data_type": "global"
                    }
                )

        @task_group(group_id=config['task_groups']['other_sourcing']['id'])
        def oracle_ps_project_sourcing(project_id: str):
            function_name = metadata[environment][f'Project_{config["customer"]["id"]}']['data_type']['project']['project']
            return GlueJobOperator(
                task_id=f"oracle_p6_sourcing_api_project_{function_name}",
                job_name=config['glue_jobs']['patterns']['sourcing_api'].format(
                    env=environment
                ),
                region_name=metadata[environment]['region'],
                script_args={
                    "--source_name": aws_config['dynamodb']['source_system_id'],
                    "--function_name": function_name,
                    "--project_id": project_id,
                    "--data_type": "project"
                }
            )

        @task_group(group_id=config['task_groups']['other_sourcing']['id'])
        def oracle_ps_other_sourcing(project_id: str):
            for function_name in metadata[environment][f'Project_{config["customer"]["id"]}']['data_type']['project']:
               if (function_name != 'project') and ('spread' not in function_name.lower()): 
                    GlueJobOperator(
                        task_id=f"oracle_p6_sourcing_api_project_{function_name}",
                        job_name=config['glue_jobs']['patterns']['sourcing_api'].format(
                            env=environment
                        ),
                        region_name=metadata[environment]['region'],
                        script_args={
                            "--source_name": aws_config['dynamodb']['source_system_id'],
                            "--function_name": function_name,
                            "--project_id": project_id,
                            "--data_type": "project"
                        }
                    )

        @task_group(group_id=config['task_groups']['spread_sourcing']['id'])
        def oracle_ps_spread_sourcing(project_id: str):
            for function_name in metadata[environment][f'Project_{config["customer"]["id"]}']['data_type']['project']:
               if ('spread' in function_name.lower()): 
                    GlueJobOperator(
                        task_id=f"oracle_p6_sourcing_api_project_{function_name}",
                        job_name=config['glue_jobs']['patterns']['sourcing_api'].format(
                            env=environment
                        ),
                        region_name=metadata[environment]['region'],
                        script_args={
                            "--source_name": aws_config['dynamodb']['source_system_id'],
                            "--function_name": function_name,
                            "--project_id": project_id,
                            "--data_type": "project"
                        }
                    )

        @task_group(group_id=config['task_groups']['raw_to_curated_global']['id'])
        def raw_to_curated_global():
            for table in metadata[environment][f'Project_{config["customer"]["id"]}']['data_type']['global'].values():
                GlueJobOperator(
                    task_id=f"cur_{aws_config['dynamodb']['source_system_id']}_{table}",
                    job_name=config['glue_jobs']['patterns']['raw_curated'].format(
                        env=environment
                    ),
                    region_name=metadata[environment]['region'],
                    script_args={
                        "--source_system_id": aws_config['dynamodb']['curation_source_system_id'],
                        "--metadata_type": f"curated#oracle_p6#{table}#job#iceberg"
                    }
                )

        @task_group(group_id=config['task_groups']['raw_to_curated_project']['id'])
        def raw_to_curated_project(project_id: str):
            for table in metadata[environment][f'Project_{config["customer"]["id"]}']['data_type']['project'].values():
                GlueJobOperator(
                    task_id=f"cur_{aws_config['dynamodb']['source_system_id']}_{table}",
                    job_name=config['glue_jobs']['patterns']['raw_curated'].format(
                        env=environment
                    ),
                    region_name=metadata[environment]['region'],
                    script_args={
                        "--source_system_id": aws_config['dynamodb']['curation_source_system_id'],
                        "--metadata_type": f"curated#oracle_p6#{table}#job#iceberg",
                        "--project_id": project_id
                    }
                )

        return oracle_ps_global_sourcing, oracle_ps_project_sourcing,oracle_ps_other_sourcing,oracle_ps_spread_sourcing,raw_to_curated_global ,raw_to_curated_project

    def create_dag(self, customer_id: str, environment: str) -> Optional[DAG]:
        """Create a DAG for specific customer and environment"""
        if customer_id not in self.customer_configs:
            print(f"No configuration found for customer {customer_id}")
            return None

        config = self.customer_configs[customer_id].config
        metadata = self._get_metadata_from_ddb(customer_id, environment)
        
        if not metadata:
            print(f"No metadata found for {customer_id}-{environment}")
            return None

        dag_id = f"oracle_p6_{customer_id.lower()}_{environment}_data_pipeline"
        
        default_args = {
            "owner": config['dag_defaults']['owner'],
            "depends_on_past": config['dag_defaults']['depends_on_past'],
            "retries": config['dag_defaults']['retries'],
            "retry_delay": datetime.timedelta(
                minutes=config['dag_defaults']['retry_delay_minutes']
            ),
            "schedule": config['dag_defaults']['schedule'],
            "email_on_failure": False,
            "email_on_retry": False,
        }

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            start_date=datetime.datetime.strptime(
                config['dag_defaults']['start_date'],
                '%Y-%m-%d'
            ),
            catchup=config['dag_defaults']['catchup'],
            max_active_runs=config['dag_defaults']['max_project_concurrency'],
            tags=config['dag_defaults']['tags']
        )

        with dag:
            # Create task groups
            global_sourcing, project_sourcing, other_sourcing , spread_sourcing ,raw_to_curated_global,raw_to_curated_project = self.create_task_groups(
                dag, customer_id, environment, metadata
            )

            # Create project control dataset
            project_control_dataset = Dataset(config['datasets']['project_control'])

            raw_crawler = GlueCrawlerOperator(
                task_id=f"oracle_p6_raw_crawler",
                config={
                "Name": f"worley-datalake-sydney-{environment}-glue-crawler-project-control-oracle-p6-raw"
                },
                poll_interval=5,
                wait_for_completion=False,
            )
    
            raw_crawler_sensor = GlueCrawlerSensor(
                task_id="wait_for_oracle_p6_raw_crawler",
                crawler_name=f"worley-datalake-sydney-{environment}-glue-crawler-project-control-oracle-p6-raw",
            )

            # Create project control task
            run_project_control_models_dag = BashOperator(
                task_id='run_project_control_models_dag',
                bash_command='echo "run project_control models dag"',
                outlets=[project_control_dataset]
            )

            # Set up task dependencies
            projects = metadata[environment][f'Project_{config["customer"]["id"]}']['Id']
            
            [global_sourcing(), 
             project_sourcing.expand(project_id=projects)] >> \
             other_sourcing.expand(project_id=projects) >> \
             spread_sourcing.expand(project_id=projects) >> raw_crawler >> raw_crawler_sensor >> \
             [raw_to_curated_global(),
             raw_to_curated_project.expand(project_id=projects)] >> \
             run_project_control_models_dag

        return dag

class CustomerDagProcessor:
    def __init__(self, config_base_path: str = "customer_configs"):
        """
        Initialize the DAG processor
        
        Args:
            config_base_path: Base path where customer configs are stored
        """
        self.config_base_path = self._resolve_config_path(config_base_path)
        self.factory = OracleP6CustomerDagFactory(self.config_base_path)
        self.processed_dags: Dict = {}

    def _resolve_config_path(self, config_path: str) -> str:
        """
        Resolve the configuration path relative to the DAG file location
        
        Args:
            config_path: The base configuration path
            
        Returns:
            str: Absolute path to the configuration directory
        """
        # Get the directory where this DAG file is located
        dag_file_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Construct absolute path to config directory
        absolute_config_path = os.path.join(dag_file_dir, config_path)
        
        if not os.path.exists(absolute_config_path):
            raise FileNotFoundError(f"Configuration directory not found: {absolute_config_path}")
            
        return absolute_config_path

    def _validate_dag(self, dag, customer_id: str, environment: str) -> bool:
        """
        Valialidate the created DAG
        
        Args:
            dag: The created DAG object
            customer_id: Customer identifier
            environment: Environment name
            
        Returns:
            bool: True if DAG is valid, False otherwise
        """
        if dag is None:
            logger.error(f"Failed to create DAG for {customer_id} - {environment}")
            return False
            
        # Add more validation as needed
        required_attributes = ['dag_id', 'default_args']
        for attr in required_attributes:
            if not hasattr(dag, attr):
                logger.error(f"DAG for {customer_id} - {environment} missing required attribute: {attr}")
                return False
                
        return True

    def process_customer_dags(self) -> Dict:
        """
        Process and create DAGs for all customers found in the config directory
        
        Returns:
            Dict: Dictionary of created DAGs
        """
        try:
            # Get all customer configs
            customer_configs = self.factory.customer_configs
            
            if not customer_configs:
                logger.warning("No customer configurations found!")
                return {}

            logger.info(f"Found {len(customer_configs)} customer configurations")

            # Process each cuscustomer configuration
            for customer_id, config in customer_configs.items():
                logger.info(f"Processing customer: {customer_id}")
                
                try:
                    # Create DAGs for each environment
                    for env in config.environments:
                        try:
                            # Create the DAG
                            dag = self.factory.create_dag(customer_id, env)
                            
                            # Validate the DAG
                            if self._validate_dag(dag, customer_id, env):
                                # Store DAG in processed_dags dictionary
                                dag_id = f"oracle_p6_{customer_id.lower()}_{env}_data_pipeline"
                                self.processed_dags[dag_id] = dag
                                
                                # Make DAG available in global namespace
                                globals()[dag_id] = dag
                                
                                logger.info(f"Successfully created DAG: {dag_id}")
                            else:
                                logger.error(f"DAG validation failed for {customer_id} - {env}")
                                
                        except Exception as env_error:
                            logger.error(f"Error processing environment {env} for customer {customer_id}: {str(env_error)}")
                            continue
                            
                except Exception as customer_error:
                    logger.error(f"Error processing customer {customer_id}: {str(customer_error)}")
                    continue

            logger.info(f"Successfully processed {len(self.processed_dags)} DAGs")
            return self.processed_dags

        except Exception as e:
            logger.error(f"Error in DAG processing: {str(e)}")
            raise

    def get_processed_dags(self) -> Dict:
        """
        Get all processed DAGs
        
        Returns:
            Dict: Dictionary of processed DAGs
        """
        return self.processed_dags

def create_all_customer_dags():
    """
    Create DAGs for all customers found in the config directory
    """
    try:
        # Initialize the processor
        processor = CustomerDagProcessor()
        
        # Process all customer DAGs
        processed_dags = processor.process_customer_dags()
        
        # Log summary
        logger.info("=== DAG Creation Summary ===")
        logger.info(f"Total DAGs created: {len(processed_dags)}")
        for dag_id in processed_dags.keys():
            logger.info(f"Created DAG: {dag_id}")
            
    except Exception as e:
        logger.error(f"Failed to create customer DAGs: {str(e)}")
        raise

# Create all customer DAGs
create_all_customer_dags()

# Make processor available for testing/debugging
dag_processor = CustomerDagProcessor()
