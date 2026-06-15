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

    // Hosts parametrizados: usan variables de entorno con default para
    // ejecucion manual (localhost). En docker-compose se sobreescriben
    // con los nombres de servicio (kafka, cassandra, spark-master).
    val sparkMaster = sys.env.getOrElse("SPARK_MASTER", "local[*]")
    val kafkaBroker = sys.env.getOrElse("KAFKA_BROKER", "localhost:29092")
    val cassandraHost = sys.env.getOrElse("CASSANDRA_HOST", "127.0.0.1")
    val basePath = sys.env.getOrElse("BASE_PATH", "s3a://lakehouse")

    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .master(sparkMaster)
      .config("spark.hadoop.fs.s3a.endpoint", sys.env.getOrElse("MINIO_ENDPOINT", "http://minio:9000"))
      .config("spark.hadoop.fs.s3a.access.key", sys.env.getOrElse("MINIO_ACCESS_KEY", "admin"))
      .config("spark.hadoop.fs.s3a.secret.key", sys.env.getOrElse("MINIO_SECRET_KEY", "admin123"))
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.change.detection.mode", "none")
      .getOrCreate()
    import spark.implicits._

    //Load the arrival delay bucketizer
    val base_path = basePath
    val arrivalBucketizerPath = "%s/models/arrival_bucketizer_2.0.bin".format(base_path)
    print(arrivalBucketizerPath.toString())
    val arrivalBucketizer = Bucketizer.load(arrivalBucketizerPath)
    val columns= Seq("Carrier","Origin","Dest","Route")

    //Load all the string field vectorizer pipelines into a dict
    val stringIndexerModelPath =  columns.map(n=> ("%s/models/string_indexer_model_"
      .format(base_path)+"%s.bin".format(n)).toSeq)
    val stringIndexerModel = stringIndexerModelPath.map{n => StringIndexerModel.load(n.toString)}
    val stringIndexerModels  = (columns zip stringIndexerModel).toMap

    // Load the numeric vector assembler
    val vectorAssemblerPath = "%s/models/numeric_vector_assembler.bin".format(base_path)
    val vectorAssembler = VectorAssembler.load(vectorAssemblerPath)

    // Load the classifier model
    val randomForestModelPath = "%s/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(
      base_path)
    val rfc = RandomForestClassificationModel.load(randomForestModelPath)

    //Process Prediction Requests in Streaming
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

    // DataFrame for Vectorizing string fields with the corresponding pipeline for that column
    val flightFlattenedDf = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance")
    flightFlattenedDf.printSchema()

    val predictionRequestsWithRouteMod = flightFlattenedDf.withColumn(
      "Route",
                concat(
                  flightFlattenedDf("Origin"),
                  lit('-'),
                  flightFlattenedDf("Dest")
                )
    )

    // Dataframe for Vectorizing numeric columns
    val flightFlattenedDf2 = flightNestedDf.selectExpr("flight.Origin",
      "flight.DayOfWeek","flight.DayOfYear","flight.DayOfMonth","flight.Dest",
      "flight.DepDelay","flight.Timestamp","flight.FlightDate",
      "flight.Carrier","flight.UUID","flight.Distance",
      "flight.Carrier_index","flight.Origin_index","flight.Dest_index","flight.Route_index")
    flightFlattenedDf2.printSchema()

    val predictionRequestsWithRouteMod2 = flightFlattenedDf2.withColumn(
      "Route",
      concat(
        flightFlattenedDf2("Origin"),
        lit('-'),
        flightFlattenedDf2("Dest")
      )
    )

    // Vectorize string fields with the corresponding pipeline for that column
    // Turn category fields into categoric feature vectors, then drop intermediate fields
    val predictionRequestsWithRoute = stringIndexerModel.map(n=>n.transform(predictionRequestsWithRouteMod))

    //Vectorize numeric columns: DepDelay, Distance and index columns
    val vectorizedFeatures = vectorAssembler.setHandleInvalid("keep").transform(predictionRequestsWithRouteMod2)

    // Inspect the vectors
    vectorizedFeatures.printSchema()

    // Drop the individual index columns
    val finalVectorizedFeatures = vectorizedFeatures
        .drop("Carrier_index")
        .drop("Origin_index")
        .drop("Dest_index")
        .drop("Route_index")

    // Inspect the finalized features
    finalVectorizedFeatures.printSchema()

    // Make the prediction
    val predictions = rfc.transform(finalVectorizedFeatures)
      .drop("Features_vec")

    // Drop the features vector and prediction metadata to give the original fields
    val finalPredictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")

    // Inspect the output
    finalPredictions.printSchema()

    // Escribir prediccion en Kafka response topic
    val kafkaWriter = finalPredictions
      .select(to_json(org.apache.spark.sql.functions.struct(finalPredictions.columns.map(col): _*)).as("value"))
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBroker)
      .option("topic", "flight-delay-ml-response")
      .option("checkpointLocation", "/tmp/kafka-response-checkpoint")
      .outputMode("append")
      .start()

    // Escribir prediccion en Cassandra
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
               (uuid, origin, dest, prediction, timestamp)
               VALUES ('$uuid', '$origin', '$dest', $prediction, toTimestamp(now()))"""
          )
        }
        session.close()
      }
      .start()

    // Console Output for predictions
    val consoleOutput = finalPredictions.writeStream
      .outputMode("append")
      .format("console")
      .start()
    consoleOutput.awaitTermination()
  }

}