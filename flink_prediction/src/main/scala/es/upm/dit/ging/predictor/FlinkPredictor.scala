package es.upm.dit.ging.predictor

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Instant
import java.util.Date

import com.datastax.oss.driver.api.core.CqlSession
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema

object FlinkPredictor {

  @JsonIgnoreProperties(ignoreUnknown = true)
  case class FlightRequest(
    UUID: String,
    FlightDate: String,
    Carrier: String,
    Origin: String,
    Dest: String,
    FlightNum: String,
    DepDelay: Double,
    Distance: Double,
    DayOfYear: Int,
    DayOfMonth: Int,
    DayOfWeek: Int,
    Timestamp: String
  )

  case class FlightPredictionResponse(
    UUID: String,
    Prediction: Int,
    Origin: String,
    Dest: String,
    Timestamp: String
  )

  case class ModelMetadata(
    modelType: String,
    numTrees: Int,
    numFeatures: Int,
    sparkVersion: String
  )

  case class BucketizerInfo(
    splits: List[Any]
  )

  case class TreeNode(
    `type`: String,
    splitType: Option[String],
    featureIndex: Option[Int],
    threshold: Option[Double],
    categories: Option[List[Double]],
    prediction: Option[Double],
    left: Option[TreeNode],
    right: Option[TreeNode]
  )

  case class TreeModel(
    treeId: Int,
    root: TreeNode
  )

  case class ExportedModel(
    metadata: ModelMetadata,
    bucketizer: BucketizerInfo,
    indexers: Map[String, List[String]],
    featuresOrder: List[String],
    forest: List[TreeModel]
  )

  def buildObjectMapper(): ObjectMapper = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper
  }

  def indexOfOrMinusOne(values: List[String], raw: String): Double = {
    values.indexOf(raw) match {
      case -1 => -1.0
      case idx => idx.toDouble
    }
  }

  def buildFeatures(req: FlightRequest, model: ExportedModel): Array[Double] = {
    val route = s"${req.Origin}-${req.Dest}"

    val carrierIndex = indexOfOrMinusOne(model.indexers.getOrElse("Carrier", Nil), req.Carrier)
    val originIndex  = indexOfOrMinusOne(model.indexers.getOrElse("Origin", Nil), req.Origin)
    val destIndex    = indexOfOrMinusOne(model.indexers.getOrElse("Dest", Nil), req.Dest)
    val routeIndex   = indexOfOrMinusOne(model.indexers.getOrElse("Route", Nil), route)

    Array(
      req.DepDelay,
      req.Distance,
      req.DayOfMonth.toDouble,
      req.DayOfWeek.toDouble,
      req.DayOfYear.toDouble,
      carrierIndex,
      originIndex,
      destIndex,
      routeIndex
    )
  }

  def evaluateTree(node: TreeNode, features: Array[Double]): Double = {
    node.`type` match {
      case "leaf" =>
        node.prediction.getOrElse(0.0)

      case "node" =>
        val idx = node.featureIndex.get
        val featureValue = features(idx)

        node.splitType.getOrElse("continuous") match {
          case "continuous" =>
            val threshold = node.threshold.get
            if (featureValue <= threshold) evaluateTree(node.left.get, features)
            else evaluateTree(node.right.get, features)

          case "categorical" =>
            val cats = node.categories.getOrElse(Nil)
            if (cats.contains(featureValue)) evaluateTree(node.left.get, features)
            else evaluateTree(node.right.get, features)

          case other =>
            throw new RuntimeException(s"Unknown splitType: $other")
        }

      case other =>
        throw new RuntimeException(s"Unknown node type: $other")
    }
  }

  def predict(features: Array[Double], model: ExportedModel): Int = {
    val votes = model.forest.map(tree => evaluateTree(tree.root, features).toInt)
    votes.groupBy(identity).maxBy(_._2.size)._1
  }

  class PredictFunction(modelPath: String) extends RichMapFunction[String, String] {
    @transient private var mapper: ObjectMapper = _
    @transient private var model: ExportedModel = _

    override def open(parameters: Configuration): Unit = {
      mapper = buildObjectMapper()
      val bytes = Files.readAllBytes(Paths.get(modelPath))
      val json = new String(bytes, StandardCharsets.UTF_8)
      model = mapper.readValue(json, classOf[ExportedModel])
    }

    override def map(value: String): String = {
      val req = mapper.readValue(value, classOf[FlightRequest])
      val features = buildFeatures(req, model)
      val pred = predict(features, model)

      val out = FlightPredictionResponse(
        UUID = req.UUID,
        Prediction = pred,
        Origin = req.Origin,
        Dest = req.Dest,
        Timestamp = req.Timestamp
      )

      mapper.writeValueAsString(out)
    }
  }

  class CassandraCustomSink(host: String) extends RichSinkFunction[String] {
    @transient private var mapper: ObjectMapper = _
    @transient private var session: CqlSession = _

    override def open(parameters: Configuration): Unit = {
      mapper = buildObjectMapper()

      session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(host, 9042))
        .withLocalDatacenter("datacenter1")
        .build()
    }

    override def invoke(value: String): Unit = {
      val pred = mapper.readValue(value, classOf[FlightPredictionResponse])

      val tsString =
        if (pred.Timestamp.endsWith("Z")) pred.Timestamp
        else pred.Timestamp + "Z"

      val ts = Date.from(Instant.parse(tsString))

      val query =
        s"""
           |INSERT INTO agile_data_science.flight_delay_predictions
           |(uuid, dest, origin, prediction, timestamp)
           |VALUES (
           |'${pred.UUID}',
           |'${pred.Dest}',
           |'${pred.Origin}',
           |${pred.Prediction},
           |'${ts.toInstant.toString}'
           |)
           |""".stripMargin.replaceAll("\n", " ")

      session.execute(query)
    }

    override def close(): Unit = {
      if (session != null) session.close()
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val source = KafkaSource.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setTopics("flight-delay-ml-request")
      .setGroupId("flink-flight-predictor")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    val stream = env.fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "Kafka Source"
    )

    val predictions = stream.map(new PredictFunction("/opt/flink/data/flink_model.json"))

    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[String]()
          .setTopic("flight-delay-ml-response")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build()

    predictions.sinkTo(kafkaSink)
    predictions.addSink(new CassandraCustomSink("cassandra"))

    env.execute("Flink Flight Delay Predictor - Phase 4")
  }
}
