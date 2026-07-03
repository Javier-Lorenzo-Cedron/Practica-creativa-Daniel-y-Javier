# !/usr/bin/env python

import sys, os, re, shutil
from os import environ
import mlflow
import mlflow.spark


# Pass date and base path to main() from airflow
def main(base_path):

  # Default to "."
  try: base_path
  except NameError: base_path = "."
  if not base_path:
    base_path = "."

  APP_NAME = "train_spark_mllib_model.py"

  # Hosts parametrizados (default localhost para desarrollo manual,
  # se sobreescriben con variables de entorno en docker-compose)
  MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
  MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
  MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
  LOCAL_MODELS_PATH = os.getenv("LOCAL_MODELS_PATH", "/app/models")
  MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
  MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "flight_delay_training")


  # If there is no SparkSession, create the environment
  try:
    sc and spark
  except (NameError, UnboundLocalError) as e:
    import pyspark
    import pyspark.sql

    spark = (
      pyspark.sql.SparkSession.builder
      .appName(APP_NAME)
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.lakehouse.type", "hadoop")
      .config("spark.sql.catalog.lakehouse.warehouse", "s3a://lakehouse/warehouse")
      .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
      .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
      .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
      .getOrCreate()
    )
    sc = spark.sparkContext

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)


  #
  # {
  #   "ArrDelay":5.0,"CRSArrTime":"2015-12-31T03:20:00.000-08:00","CRSDepTime":"2015-12-31T03:05:00.000-08:00",
  #   "Carrier":"WN","DayOfMonth":31,"DayOfWeek":4,"DayOfYear":365,"DepDelay":14.0,"Dest":"SAN","Distance":368.0,
  #   "FlightDate":"2015-12-30T16:00:00.000-08:00","FlightNum":"6109","Origin":"TUS"
  # }
  #
  from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
  from pyspark.sql.types import StructType, StructField
  from pyspark.sql.functions import udf

  schema = StructType([
    StructField("ArrDelay", DoubleType(), True),     # "ArrDelay":5.0
    StructField("CRSArrTime", TimestampType(), True),    # "CRSArrTime":"2015-12-31T03:20:00.000-08:00"
    StructField("CRSDepTime", TimestampType(), True),    # "CRSDepTime":"2015-12-31T03:05:00.000-08:00"
    StructField("Carrier", StringType(), True),     # "Carrier":"WN"
    StructField("DayOfMonth", IntegerType(), True), # "DayOfMonth":31
    StructField("DayOfWeek", IntegerType(), True),  # "DayOfWeek":4
    StructField("DayOfYear", IntegerType(), True),  # "DayOfYear":365
    StructField("DepDelay", DoubleType(), True),     # "DepDelay":14.0
    StructField("Dest", StringType(), True),        # "Dest":"SAN"
    StructField("Distance", DoubleType(), True),     # "Distance":368.0
    StructField("FlightDate", DateType(), True),    # "FlightDate":"2015-12-30T16:00:00.000-08:00"
    StructField("FlightNum", StringType(), True),   # "FlightNum":"6109"
    StructField("Origin", StringType(), True),      # "Origin":"TUS"
  ])

  def delete_if_exists(path):
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)

  with mlflow.start_run(run_name="spark_random_forest_training"):

    mlflow.log_param("app_name", APP_NAME)
    mlflow.log_param("minio_endpoint", MINIO_ENDPOINT)
    mlflow.log_param("training_table", "lakehouse.flights_db.flights")
    mlflow.log_param("model_type", "RandomForestClassifier")
    mlflow.log_param("categorical_columns", "Carrier,Origin,Dest,Route")
    mlflow.log_param("model_storage", "s3a://lakehouse/models")

    # Leemos los datos desde la tabla Iceberg del Lakehouse
    features = spark.table("lakehouse.flights_db.flights").repartition(8).cache()
    training_rows = features.count()

    mlflow.log_metric("training_rows", training_rows)

    print("Partitions in features:", features.rdd.getNumPartitions())

    # Check for nulls in features before using Spark ML
    null_counts = [(column, features.where(features[column].isNull()).count()) for column in features.columns]
    cols_with_nulls = filter(lambda x: x[1] > 0, null_counts)
    print(list(cols_with_nulls))

    from pyspark.sql.functions import lit, concat

    features_with_route = features.withColumn(
      'Route',
      concat(
        features.Origin,
        lit('-'),
        features.Dest
      )
    )
    features_with_route.show(6)

    from pyspark.ml.feature import Bucketizer

    splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
    mlflow.log_param("arrival_delay_bucket_splits", str(splits))

    arrival_bucketizer = Bucketizer(
      splits=splits,
      inputCol="ArrDelay",
      outputCol="ArrDelayBucket"
    )

    arrival_bucketizer_path = "s3a://lakehouse/models/arrival_bucketizer_2.0.bin"
    arrival_bucketizer.write().overwrite().save(arrival_bucketizer_path)
    mlflow.log_param("arrival_bucketizer_path", arrival_bucketizer_path)

    ml_bucketized_features = arrival_bucketizer.transform(features_with_route)
    ml_bucketized_features.select("ArrDelay", "ArrDelayBucket").show()

    from pyspark.ml.feature import StringIndexer, VectorAssembler

    categorical_columns = ["Carrier", "Origin", "Dest", "Route"]

    for column in categorical_columns:
      string_indexer = StringIndexer(
        inputCol=column,
        outputCol=column + "_index"
      )

      string_indexer_model = string_indexer.fit(ml_bucketized_features)
      ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)

      ml_bucketized_features = ml_bucketized_features.drop(column)

      string_indexer_model_path = f"s3a://lakehouse/models/string_indexer_model_{column}.bin"
      string_indexer_model.write().overwrite().save(string_indexer_model_path)
      mlflow.log_param(f"string_indexer_model_{column}_path", string_indexer_model_path)

    numeric_columns = [
      "DepDelay", "Distance",
      "DayOfMonth", "DayOfWeek",
      "DayOfYear"
    ]

    index_columns = [
      "Carrier_index", "Origin_index",
      "Dest_index", "Route_index"
    ]

    mlflow.log_param("numeric_columns", ",".join(numeric_columns))
    mlflow.log_param("index_columns", ",".join(index_columns))

    vector_assembler = VectorAssembler(
      inputCols=numeric_columns + index_columns,
      outputCol="Features_vec"
    )

    final_vectorized_features = vector_assembler.transform(ml_bucketized_features)

    vector_assembler_path = "s3a://lakehouse/models/numeric_vector_assembler.bin"
    vector_assembler.write().overwrite().save(vector_assembler_path)
    mlflow.log_param("vector_assembler_path", vector_assembler_path)

    for column in index_columns:
      final_vectorized_features = final_vectorized_features.drop(column)

    final_vectorized_features.show()

    from pyspark.ml.classification import RandomForestClassifier

    max_bins = 4657
    max_memory_mb = 1024

    mlflow.log_param("maxBins", max_bins)
    mlflow.log_param("maxMemoryInMB", max_memory_mb)

    rfc = RandomForestClassifier(
      featuresCol="Features_vec",
      labelCol="ArrDelayBucket",
      predictionCol="Prediction",
      maxBins=max_bins,
      maxMemoryInMB=max_memory_mb
    )

    final_vectorized_features = final_vectorized_features.repartition(8).cache()
    final_vectorized_features.count()

    print("Partitions in training DF:", final_vectorized_features.rdd.getNumPartitions())

    model = rfc.fit(final_vectorized_features)

    model_path = "s3a://lakehouse/models/spark_random_forest_classifier.flight_delays.5.0.bin"

    # Guardar el modelo operativo en el Lakehouse / MinIO
    model.write().overwrite().save(model_path)
    mlflow.log_param("lakehouse_model_path", model_path)

    # Evaluate model using test data
    predictions = model.transform(final_vectorized_features)

    from pyspark.ml.evaluation import MulticlassClassificationEvaluator

    evaluator = MulticlassClassificationEvaluator(
      predictionCol="Prediction",
      labelCol="ArrDelayBucket",
      metricName="accuracy"
    )

    accuracy = evaluator.evaluate(predictions)

    print("Accuracy = {}".format(accuracy))

    mlflow.log_metric("accuracy", accuracy)

    predictions.groupBy("Prediction").count().show()

    predictions.sample(False, 0.001, 18).orderBy("CRSDepTime").show(6)

    # Registrar el modelo también en MLflow
    # Si esta línea diese problemas por dependencias, puedes comentarla.
    mlflow.spark.log_model(
      spark_model=model,
      artifact_path="random_forest_model"
    )

if __name__ == "__main__":
  main(sys.argv[1])