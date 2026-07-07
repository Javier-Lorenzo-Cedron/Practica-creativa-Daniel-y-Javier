package es.upm.dit.ging.predictor

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.sink.{KafkaRecordSerializationSchema, KafkaSink}
import org.apache.flink.api.common.serialization.SimpleStringSchema

object FlinkPredictor {
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

    val echoed = stream.map(msg => msg)

    val kafkaSink = KafkaSink.builder[String]()
      .setBootstrapServers("kafka:9092")
      .setRecordSerializer(
        KafkaRecordSerializationSchema.builder[String]()
          .setTopic("flight-delay-ml-response")
          .setValueSerializationSchema(new SimpleStringSchema())
          .build()
      )
      .build()

    echoed.sinkTo(kafkaSink)

    env.execute("Flink Flight Delay Predictor - Phase 1")
  }
}
