package es.upm.dit.ging.predictor

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
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

    env.execute("Flink Flight Delay Predictor - Phase 2")
  }
}
