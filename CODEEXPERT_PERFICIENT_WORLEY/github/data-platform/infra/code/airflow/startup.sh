#!/bin/sh

export DBT_VENV_PATH="${AIRFLOW_HOME}/dbt_venv"
export DBT_PROJECT_PATH="${AIRFLOW_HOME}/dags/dbt"
export PIP_USER=false

python3 -m venv "${DBT_VENV_PATH}"

${DBT_VENV_PATH}/bin/pip install dbt-redshift
${DBT_VENV_PATH}/bin/pip install dbt-glue

export PIP_USER=true
