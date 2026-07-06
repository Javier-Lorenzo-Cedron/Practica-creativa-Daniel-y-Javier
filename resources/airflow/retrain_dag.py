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
    "--deploy-mode client "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
    "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
    "--conf spark.hadoop.fs.s3a.access.key=admin "
    "--conf spark.hadoop.fs.s3a.secret.key=admin123 "
    "--conf spark.hadoop.fs.s3a.path.style.access=true "
    "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
    "--conf spark.hadoop.fs.s3a.endpoint.region=us-east-1 "
    "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
    "--conf spark.hadoop.fs.s3a.change.detection.mode=none "
    "--packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0,org.apache.hadoop:hadoop-aws:3.4.1 "
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
        env={
            "MLFLOW_TRACKING_URI": "http://mlflow:5000",
            "MLFLOW_EXPERIMENT_NAME": "flight_delay_training",
            "MINIO_ENDPOINT": "http://minio:9000",
            "MINIO_ACCESS_KEY": "admin",
            "MINIO_SECRET_KEY": "admin123",
        },
    )