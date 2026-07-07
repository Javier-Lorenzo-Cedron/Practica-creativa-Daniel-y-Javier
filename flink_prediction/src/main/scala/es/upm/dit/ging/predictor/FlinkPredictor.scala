package es.upm.dit.ging.predictor

import java.net.InetSocketAddress
import java.time.Instant
import java.util.Date

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.PreparedStatement
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

  class PredictFunction extends RichMapFunction[String, String] {
    @transient private var mapper: ObjectMapper = _

    override def open(parameters: Configuration): Unit = {
      mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    override def map(value: String): String = {
      val req = mapper.readValue(value, classOf[FlightRequest])

      val out = FlightPredictionResponse(
        UUID = req.UUID,
        Prediction = 1,
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
    @transient private var prepared: PreparedStatement = _

    override def open(parameters: Configuration): Unit = {
      mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

      session = CqlSession.builder()
        .addContactPoint(new InetSocketAddress(host, 9042))
        .withLocalDatacenter("datacenter1")
        .build()

      prepared = session.prepare(
        """
        INSERT INTO agile_data_science.flight_delay_predictions
        (uuid, dest, origin, prediction, timestamp)
        VALUES (?, ?, ?, ?, ?)
        """
      )
    }

    override def invoke(value: String): Unit = {
      val pred = mapper.readValue(value, classOf[FlightPredictionResponse])

      val tsString =
        if (pred.Timestamp.endsWith("Z")) pred.Timestamp
        else pred.Timestamp + "Z"

      val ts = Date.from(Instant.parse(tsString))

      val bound = prepared.bind(
        pred.UUID,
        pred.Dest,
        pred.Origin,
        Int.box(pred.Prediction),
        ts
      )

      session.execute(bound)
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

    val predictions = stream.map(new PredictFunction())

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

    env.execute("Flink Flight Delay Predictor - Phase 3")
  }
}
