# Worley Construction Domain Data Models
The Worley Construction Domain Data Models repository is where you store your integrated domain model files (DBT) and Amazon Managed Workflows for Apache Airflow's (mwaa) directed acyclic graph (DAG) file.

## Data Build Tool (dbt)
It is an open-source analytics engineering tool that enables data engineers to transform data in the data warehouse. It focuses on the transformation layer of the data stack, allowing users to model, test, document, and execute SQL-based transformations in the data warehouse.

## Amazon Managed Workflows for Apache Airflow (mwaa)
MWAA orchestrates workflows and data pipelines. It allows users to define, schedule, and monitor workflows as directed acyclic graphs (DAGS) of tasks.

## Getting Started
To get started, clone the project repository:
```bash
git clone https://github.com/Worley-AWS/construction-domain-model.git
```

Create a python virtual environment in the root directory
```bash
python3 -m venv .venv
```

Activate the virtual environment and install the DBT dependencies.
```bash
source .venv/bin/activate
pip install -r requirements
```

## Table Standards

1. Primary Key :
    - When a table has a single field as a unique attribute, a new column called **<'table'>_key'** is created with this field which can be used the primary key column.
    - When a table has multiple fields as a unique attribute, a new column called
    **<'table'>_key'** is created with the md5 hash of those fields  which can be used the primary key column.

2. Audit Columns :
    - execution_date : This is the audit column which is being propogated from upstream curated layer which denotes the date on which the record was created.
    - source_name : This is the audit column which is being propogated from upstream curated layer which denotes the source this record belongs to.
    - created_date : This is the audit column which is being added in the Integrated Domain Layer which denotes the date on which record was created.
    - updated_date : This is the audit column which is being added in the Integrated Domain Layer which denotes the date on which the record was last updated.
    - load_id :  This is the audit column which is being added in the Integrated Domain Layer. This load_id field indicates the record was loaded or modified as part of which load that can be used for debugging.

<div class="note">
  <strong>Note:</strong> When a record is created for the first time, the created_date and updated_date would be same. This behaviour ensures that updated_date is not nullable and downstream consumers can use the updated_date to pickup the changes in the tables.
</div>

## GitHub Action File
In .github/workflows, there is a GitHub action file named "publish_assets.yml". This is the ci/cd file to sync all model files and DAG to DEV and QA environments. When setting up new repositories, copy and paste this file, change the s3 prefix to point to where your model files should reside. For example, for construction user case, it should sit in the "construction" prefix of the s3 bucket in the yml file.

## Redshift DBA Set Up
Before we deploy the model files and DAG file to the AWS environment, we need to ensure Redshift DBA and data governance team have completed the pre-requisite set up. This is to create the database objects, e.g. databases and schemas where we will source tables from and build our models. It also ensures the dbt job has the right permissions to access and use these database objects in order to create the models.  

This involves defining the data_governance.json file in the data-governance repo and creating a secret for the dbt user. Here is the link to the data-governance repo README on what the Redshift DBA and data governance team need to do - https://github.com/Worley-AWS/data-governance/blob/main/README.md

## DAG Set Up
<strong> Secrets Manager Variable </strong>

In the DAG file, there is a step to retrieve the secret manager name for the dbt user credentials, which is "dag_config = Variable.get("dbt_user_name_config", deserialize_json=True)".

Before you deploy the DAG file, you need to go to airflow UI and store the dbt user secret name:
1. Go to "Admin" at the top
2. Select "Variables"
3. Go to "dbt_user_name_config"
4. Insert a new entry for the domain and its secret manager secret name
5. Save

Once you store the secret name in this Variable file, the DAG will be able to fetch the name in execution time and obtain the credentials from secrets manager.

<strong> Redshift Connection </strong>

For each database, you need to create a Redshift connection:
1. Go to Airflow UI in AWS console
2. Click "Admin" at the top, select "Connections"
3. Click "Add a new record" button
4. For "Connection Id", create a name for this connection
5. For "Connection Type", choose "Amazon Redshift"
6. For "Host", put in the Endpoint of the Redshift cluster without the port and database name, e.g. "redshift-cluster-1.xxxxxx.us-east-1.redshift.amazonaws.com"
7. For "Database", put in the Database of the Redshift cluster
8. For "Port", put in the Port of the Redshift cluster

## How to Configure the Sample DAG
Here is an overview of what each component of the dag does:

- ExecutionConfig: Configuration for dbt execution, which specifies the path to the dbt executable and the execution mode (virtual environment).
- ProfileConfig: Configuration for dbt profiles, specifying the profile name, target name, and connection details for Redshift.
- ProjectConfig: Configuration for dbt projects, specifying the path to the dbt project.
- The DAG class: this is instantiated to define the overall structure of the DAG with a few parameters:
  - dag_id: Unique identifier for the DAG.
  - start_date: The start date for the DAG.
  - schedule_interval: The frequency with which the DAG should run (@daily means it runs once a day).
  - tags: The tags to identify the DAG.
  - catchup: An Airflow DAG defined with a start_date, possibly an end_date, and a non-dataset schedule, defines a series of intervals which the scheduler turns into individual DAG runs and executes. The scheduler, by default, will kick off a DAG Run for any data interval that has not been run since the last data interval (or has been cleared). This concept is called Catchup. If your DAG is not written to handle its catchup (i.e., not limited to the interval, but instead to Now for instance.), then you will want to turn catchup off. This can be done by setting catchup=False in DAG. When turned off, the scheduler creates a DAG run only for the latest interval.

Here is a list of items you need to check and configure according to your use case:

- Set the "PROJECT_NAME" variable is to the correct project. In this case, it should be set to "construction" as it is a repository to store construction files.
- Confirm the "conn_id" from ProfileConfig matches the "Connection ID" of the Redshift connection. For example, if the "Connection ID" of the Redshift connection is "redshift_construction_connection", make sure "conn_id" from ProfileConfig is "redshift_construction_connection".
- Confirm the "profile_args" from ProfileConfig contains the correct Redshift schema for your modelling files to be uploaded to. In this case, it should be the target schema to store dac objects. For example, if the models should belong in "domain_integrated_model" schema in Redshift, make sure the "schema" in "profile_args" from ProfileConfig is set to "domain_integrated_model".
- Configure DAG class to set:
  - any dag name you want to use ("dag_id")
  - when to start the dag ("start_date")
  - the schedule to run the dag ("schedule_interval")
  - whether to have catch up ("catchup")
  - tags ("tags")
- Add any additional configurations if required by any business requirement

## Prerequisites
- Basic understanding of SQL
- Basic understanding of DBT Syntax
