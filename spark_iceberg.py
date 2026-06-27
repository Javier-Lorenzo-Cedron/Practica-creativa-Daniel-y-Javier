import os
from pyspark.sql import SparkSession

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
BUCKET = os.getenv("MINIO_BUCKET", "lakehouse")

spark = (
    SparkSession.builder
    .appName("IcebergSetup")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.type", "hadoop")
    .config("spark.sql.catalog.lakehouse.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
    .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.json(f"s3a://{BUCKET}/raw/simple_flight_delay_features.jsonl.bz2")
print(f"Registros cargados: {df.count()}")

spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.flights_db")

(
    df.writeTo("lakehouse.flights_db.flights")
    .using("iceberg")
    .tableProperty("write.format.default", "parquet")
    .createOrReplace()
)

print("Tabla Iceberg creada en MinIO correctamente.")
spark.stop()
