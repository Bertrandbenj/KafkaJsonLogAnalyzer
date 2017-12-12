/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.nimajneb.kafka

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.logging.log4j._
import org.apache.logging.log4j.core.layout._
import org.apache.logging.log4j.scala._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaConsumer <brokers> <topics>
  *   <brokers> is a list of one or more Kafka brokers
  *   <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  *    $ bin/run-example streaming.DKafkaConsumer broker1-host:port,broker2-host:port \
  *    topic1,topic2
  */
object DKafkaConsumer extends Logging{
  logger.traceEntry("Entering DKafkaConsumer")
  val DKConsumer : Marker = MarkerManager.getMarker("DKafkaConsumer")

  Utils.setStreamingLogLevels()
  val spark = SparkSession
    .builder
    .master("local[*]")
    .appName("KafkaJsonLogParser")
    .getOrCreate()
  import spark.implicits._
  spark.sparkContext.addJar("/home/ben/.m2/repository/org/apache/spark/spark-streaming-kafka-0-10_2.11/2.2.0/spark-streaming-kafka-0-10_2.11-2.2.0.jar")
  spark.sparkContext.addJar("/home/ben/.m2/repository/org/apache/kafka/kafka-clients/0.10.0.0/kafka-clients-0.10.0.0.jar")
  spark.sparkContext.addJar("target/kafkalogging-1.0.jar")

  logger.trace(spark.sparkContext.listJars().mkString("\n"))

  def main(args: Array[String]) {
    logger.traceEntry("Entering main")

    // Handle arguments
    if (args.length < 2) {
      logger.error(s"""
                            |Usage: DKafkaConsumer <brokers> <topics>
                            |  <brokers> is a list of one or more Kafka brokers
                            |  <topics> is a list of one or more kafka topics to consume from
                            |
        """.stripMargin)
      System.exit(1)
    }
    val Array(brokers, topics) = args

    // Set kafka params
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val sqlContext = new SQLContext(spark.sparkContext)


    // Create Streaming context with 60 seconds batch interval
    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    ssc.checkpoint("chkPt")

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "bootstrap.servers" -> brokers)
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    logger.info(DKConsumer,"messages.id="+messages.id+";topics="+topicsSet)


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .load()

    logger.debug(DKConsumer,"df="+df)
    import DFUtils._
    val req = DKafkaSupervision
      .recursiveExplodeMarkers(
        df.selectExpr("CAST(value AS STRING)").as[String]
        .select(from_json($"value", JsonLogSchema.struct).as("data"))
        .select("data.*")
      )
      .asTime()
    logger.debug(DKConsumer,"req="+req)
    val t1 = req.log

    val t2 = req
      .propertiesLikeMessage("project1.joinAandB.count")
      .log

    t1.awaitTermination()
    t2.awaitTermination()


    //messages.map(from_json(_.value).cast("string"), struct)

    val lines = messages.map(_.value.trim())
    logger.info(DKConsumer,"lines.count="+lines.count())


    DKafkaSupervision.extract(lines)
    //DKafkaErrors.extract(lines)


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    logger.traceExit(DKConsumer)
  }

  implicit class DFImpr (val df: DataFrame){
    def log(): StreamingQuery = {
      df.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      //.awaitTermination()
    }
  }
}
// scalastyle:on println