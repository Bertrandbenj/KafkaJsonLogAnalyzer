package com.nimajneb.kafka

import org.apache.logging.log4j.{Marker, MarkerManager}
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import DKafkaConsumer.spark
import DKafkaConsumer.spark.implicits._



object DKafkaErrors extends Logging {

  val sqlContext = new SQLContext(spark.sparkContext)

  val DKErrors: Marker = MarkerManager.getMarker("DKErrors")

  def extract(stream: DStream[String]): Unit = {
    logger.traceEntry()
    stream.repartition(1).foreachRDD(rdd => {
      if (!rdd.isEmpty())
        transform(rdd)
          .write
          .mode(SaveMode.Overwrite)
          .json("errors")
    })
    logger.traceExit()
  }

  def transform(rdd: RDD[String]): DataFrame = {

    logger.trace(DKErrors, "DKErrors.transform")
    logger.info(DKErrors, "lines.count=" + rdd.count())

    val fullLog = spark.read.json(rdd)

    fullLog.filter(
      $"level" === "ERROR" ||
        $"level" === "TRACE" ||
        $"level" === "WARN" ||
        $"level" === "FATAL")

  }
}
