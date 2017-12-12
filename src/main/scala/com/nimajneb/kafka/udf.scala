package com.nimajneb.kafka

import DKafkaConsumer.spark
import org.apache.logging.log4j.Marker

object udf {
  spark.udf.register("hasMarker", (input: String, m: String) => input.toUpperCase)
}
