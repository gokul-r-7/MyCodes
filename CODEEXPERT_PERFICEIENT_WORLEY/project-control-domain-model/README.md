# Worley Project Control Domain Data Models
The Worley Project Control Domain Data Models repository is where you store your integrated domain model files (DBT) and Amazon Managed Workflows for Apache Airflow's (mwaa) directed acyclic graph (DAG) file.

## Data Build Tool (dbt)
It is an open-source analytics engineering tool that enables data engineers to transform data in the data warehouse. It focuses on the transformation layer of the data stack, allowing users to model, test, document, and execute SQL-based transformations in the data warehouse.

## Amazon Managed Workflows for Apache Airflow (mwaa)
MWAA orchestrates workflows and data pipelines. It allows users to define, schedule, and monitor workflows as directed acyclic graphs (DAGS) of tasks.

## Getting Started
To get started, clone the project repository:
```bash
git clone https://github.com/Worley-AWS/project-control-domain-model.git
```

Create a python virtual environment in the root directory
```bash
python3 -m venv .venv
```

Activate the virtual environment and install the DBT dependencies.
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

## Table Standards

1. Primary Key :
    - When a table has a single field as a unique attribute, a new column called **<'table'>_key'** is created with this field which can be used the primary key column.
    - When a table has multiple fields as a unique attribute, a new column called
    **<'table'>_key'** is created with the md5 hash of those fields  which can be used the primary key column.

2. Audit Columns :
    - execution_date : This is the audit column which is being propogated from upstream curated layer which denotes the date on which the record was created.
    - source_name : This is the audit column which is being propogated from upstream curated layer which denotes the source this record belongs to.
    - modified_date : This is the audit column which is being added in the Integrated Domain Layer. If there is any downstream system or scenario for which a record has to be updated, the date of the update operation need to be populated here.
    - load_id : This is the audit column which is being added in the Integrated Domain Layer. This load_id field indicates the record was loaded or modified as part of which load which can be used for debugging.


## Prerequisites
- Basic understanding of SQL
- Basic understanding of DBT Syntax