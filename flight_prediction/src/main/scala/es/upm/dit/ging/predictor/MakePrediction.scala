package es.upm.dit.ging.predictor

import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.feature.{Bucketizer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{concat, from_json, lit, to_json, col}
import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress

object MakePrediction {

  def main(args: Array[String]): Unit = {
    println("Fligth predictor starting...")

    val sparkMaster = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "localhost:29092")
    val cassandraHost = sys.env.getOrElse("CASSANDRA_HOST", "127.0.0.1")
    val basePath = sys.env.getOrElse("BASE_PATH", "s3a://lakehouse")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master(sparkMaster)
      .config("spark.hadoop.fs.s3a.endpoint", sys.env.getOrElse("MINIO_ENDPOINT", "[minio](http://minio:9000)"))
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("MINIO_ACCESS_KEY", "admin"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("MINIO_SECRET_KEY", "admin123"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
      .getOrCreate()

    import spark.implicits._

    // Load the arrival delay bucketizer
    val arrivalBucketizerPath = s"$basePath/models/arrival_bucketizer_2.0.bin"
    println(arrivalBucketizerPath)
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)

    val columns = Seq("Carrier", "Origin", "Dest", "Route")

    // Load all the string field vectorizer pipelines into a dict
    val stringIndexerModelPaths = columns.map(n => s"$basePath/models/string_indexer_model_$n.bin")
    val stringIndexerModels = (columns zip stringIndexerModelPaths.map(StringIndexerModel.load)).toMap

    // Load the numeric vector assembler
    val vectorAssemblerPath = s"$basePath/models/numeric_vector_assembler.bin"
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    // Load the classifier model
    val randomForestModelPath = s"$basePath/models/spark_random_forest_classifier.flight_delays.5.0.bin"
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    // Process Prediction Requests in Streaming
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("subscribe", "flight-delay-ml-request")
      .option("startingOffsets", "earliest")
      .load()

    df.printSchema()

    val flightJsonDf = df.selectExpr("CAST(value AS STRING)")

    val struct = new StructType()
      .add("Origin", DataTypes.StringType)
      .add("FlightNum", DataTypes.StringType)
      .add("DayOfWeek", DataTypes.IntegerType)
      .add("DayOfYear", DataTypes.IntegerType)
      .add("DayOfMonth", DataTypes.IntegerType)
      .add("Dest", DataTypes.StringType)
      .add("DepDelay", DataTypes.DoubleType)
      .add("Prediction", DataTypes.StringType)
      .add("Timestamp", DataTypes.TimestampType)
      .add("FlightDate", DataTypes.DateType)
      .add("Carrier", DataTypes.StringType)
      .add("UUID", DataTypes.StringType)
      .add("Distance", DataTypes.DoubleType)
      .add("Carrier_index", DataTypes.DoubleType)
      .add("Origin_index", DataTypes.DoubleType)
      .add("Dest_index", DataTypes.DoubleType)
      .add("Route_index", DataTypes.DoubleType)

    val flightNestedDf = flightJsonDf.select(from_json($"value", struct).as("flight"))
    flightNestedDf.printSchema()

    val flightFlattenedDf = flightNestedDf.selectExpr(
      "flight.Origin",
      "flight.FlightNum",
      "flight.DayOfWeek",
      "flight.DayOfYear",
      "flight.DayOfMonth",
      "flight.Dest",
      "flight.DepDelay",
      "flight.Timestamp",
      "flight.FlightDate",
      "flight.Carrier",
      "flight.UUID",
      "flight.Distance"
    )

    flightFlattenedDf.printSchema()

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route",
      concat(
        flightFlattenedDf("Origin"),
        lit('-'),
        flightFlattenedDf("Dest")
      )
    )

    // Vectorize string fields with the corresponding pipeline for that column
    val predictionRequestsIndexed = columns.foldLeft(predictionRequestsWithRouteMod) {
      case (dfTmp, colName) => stringIndexerModels(colName).transform(dfTmp)
    }

    // Vectorize numeric columns: DepDelay, Distance and index columns
    val vectorizedFeatures = vectorAssembler
      .setHandleInvalid("keep")
      .transform(predictionRequestsIndexed)

    vectorizedFeatures.printSchema()

    val finalVectorizedFeatures = vectorizedFeatures
      .drop("Carrier_index")
      .drop("Origin_index")
      .drop("Dest_index")
      .drop("Route_index")

    finalVectorizedFeatures.printSchema()

    // Make prediction
    val predictions = rfc.transform(finalVectorizedFeatures)
      .withColumnRenamed("prediction", "Prediction")
      .drop("Features_vec")

    val finalPredictions = predictions
      .drop("indices")
      .drop("values")
      .drop("rawPrediction")
      .drop("probability")

    finalPredictions.printSchema()

    // Write prediction to Kafka response topic
    val kafkaWriter = finalPredictions
      .select(to_json(org.apache.spark.sql.functions.struct(finalPredictions.columns.map(col): _*)).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/kafka-response-checkpoint")
      .outputMode("append")
      .start()

    // Write prediction to Cassandra
    val cassandraWriter = finalPredictions.writeStream
      .outputMode("append")
      .option("checkpointLocation", "/tmp/cassandra-checkpoint")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val session = CqlSession.builder()
          .addContactPoint(new InetSocketAddress(cassandraHost, 9042))
          .withLocalDatacenter("datacenter1")
          .build()

        batchDF.collect().foreach { row =>
          val uuid = row.getAs[String]("UUID")
          val origin = row.getAs[String]("Origin")
          val dest = row.getAs[String]("Dest")
          val prediction = row.getAs[Double]("Prediction").toInt

          session.execute(
            s"""INSERT INTO agile_data_science.flight_delay_predictions
               |(uuid, origin, dest, prediction, timestamp)
               |VALUES ('$uuid', '$origin', '$dest', $prediction, toTimestamp(now()))""".stripMargin
          )
        }

        session.close()
      }
      .start()

    // Console output for debugging
    val consoleOutput = finalPredictions.writeStream
      .outputMode("append")
      .format("console")
      .start()

    consoleOutput.awaitTermination()
  }
}
