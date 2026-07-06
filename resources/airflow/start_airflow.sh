#!/bin/bash
set -e

export AIRFLOW_HOME=/opt/airflow
export AIRFLOW__CORE__EXECUTOR=SequentialExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////opt/airflow/airflow.db
export AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags

airflow db migrate

airflow users create --username admin --password admin \
    --firstname Admin --lastname Admin --role Admin \
    --email admin@example.com || true

airflow scheduler &
exec airflow webserver --port 8080