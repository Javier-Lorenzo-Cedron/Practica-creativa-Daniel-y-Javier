from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "javier",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

SPARK_SUBMIT = (
    "/opt/spark/bin/spark-submit "
    "--master spark://spark-master:7077 "
    "/opt/airflow/project/resources/train_spark_mllib_model.py "
    "/opt/airflow/project"
)

with DAG(
    dag_id="flight_delay_retraining",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["ibdn", "spark", "mllib"],
) as dag:
    retrain = BashOperator(
        task_id="retrain_model",
        bash_command=SPARK_SUBMIT,
    )