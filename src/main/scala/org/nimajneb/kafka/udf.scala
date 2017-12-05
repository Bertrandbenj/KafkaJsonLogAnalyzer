package org.nimajneb
import org.nimajneb.kafka.DKafkaConsumer.spark
import org.apache.logging.log4j.Marker

object udf {
  spark.udf.register("hasMarker", (input: String, m: String) => input.toUpperCase)
}
