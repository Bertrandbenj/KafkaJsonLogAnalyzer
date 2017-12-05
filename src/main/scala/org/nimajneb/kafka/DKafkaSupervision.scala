package org.nimajneb.kafka

import org.apache.logging.log4j.{Marker, MarkerManager}
import org.apache.logging.log4j.scala._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType
import org.apache.spark.streaming.dstream.DStream
import org.nimajneb.kafka.DKafkaConsumer.spark.implicits._
import org.nimajneb.kafka.DKafkaConsumer.spark
import org.nimajneb.kafka.DFUtils._

/**
  * root
  * |-- contextMap: array (nullable = true)
  * |    |-- element: string (containsNull = true)
  * |-- endOfBatch: boolean (nullable = true)
  * |-- level: string (nullable = true)
  * |-- loggerFqcn: string (nullable = true)
  * |-- loggerName: string (nullable = true)
  * |-- marker: struct (nullable = true)
  * |    |-- name: string (nullable = true)
  * |    |-- parents: array (nullable = true)
  * |    |    |-- element: struct (containsNull = true)
  * |    |    |    |-- name: string (nullable = true)
  * |    |    |    |-- parents: array (nullable = true)
  * |    |    |    |    |-- element: struct (containsNull = true)
  * |    |    |    |    |    |-- name: string (nullable = true)
  * |-- message: string (nullable = true)
  * |-- source: struct (nullable = true)
  * |    |-- class: string (nullable = true)
  * |    |-- file: string (nullable = true)
  * |    |-- line: long (nullable = true)
  * |    |-- method: string (nullable = true)
  * |-- thread: string (nullable = true)
  * |-- threadId: long (nullable = true)
  * |-- threadPriority: long (nullable = true)
  * |-- thrown: struct (nullable = true)
  * |    |-- extendedStackTrace: array (nullable = true)
  * |    |    |-- element: struct (containsNull = true)
  * |    |    |    |-- class: string (nullable = true)
  * |    |    |    |-- exact: boolean (nullable = true)
  * |    |    |    |-- file: string (nullable = true)
  * |    |    |    |-- line: long (nullable = true)
  * |    |    |    |-- location: string (nullable = true)
  * |    |    |    |-- method: string (nullable = true)
  * |    |    |    |-- version: string (nullable = true)
  * |    |-- localizedMessage: string (nullable = true)
  * |    |-- message: string (nullable = true)
  * |    |-- name: string (nullable = true)
  * |-- timeMillis: long (nullable = true)
  */

case class MarkerEntry(name: Int, parentId: Option[Int])

object DKafkaSupervision extends Logging {

  val DKSup: Marker = MarkerManager.getMarker("DKafkaSupervision").addParents(KLoggerGenerator.SUPERVISION)

  val DKSup2: Marker = MarkerManager.getMarker("ANC3").addParents(DKSup)

  def extract(stream: DStream[String]): Unit = {
    //stream.map(spark.read.json(_)).print()
    stream.repartition(1).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val json = spark.read.json(rdd).cache
        val time = json.sort("timeMillis").select("timeMillis").first()
        val messages = json.clean().cache()


        val parseMarkers = recursiveExplodeMarkers(messages)
        parseMarkers.show()
        parseMarkers.select("level", "markers", "time", "message", "src", "loggerName")
          .toExcel(time + "/Markers.xlsx")

        val properties = parseMarkers.propertiesLikeMessage
        properties.printSchema()
        properties.show()
        properties.toExcel(time +"1/Properties.xlsx")
      }

    })
  }

  def transform(fullLog: DataFrame): DataFrame = {

    //logger.warn(DKSup,fullLog.schema.treeString)

    logger.traceEntry
    logger.info(DKSup2, "fullLog.count=" + fullLog.count())

    logger.info("markers ... ")
    val withMarkers = flattenMarkers(fullLog)
    withMarkers.show()

    logger.info(KLoggerGenerator.FLOW, "markers ... ")

    val res = withMarkers
      .filter($"level" === "INFO" || $"level" === "TRACE")
      .select($"markers",
        $"level",
        from_unixtime($"timeMillis" / 1000).as("time"),
        $"message",
        $"source")
      .withColumn("codeLoc", concat($"source.file", lit(":"), $"source.line", lit(":"), $"source.method"))
      .drop("source", "contextMap")

    //res.show(false)

    logger.info(DKSup, "supervision.out.count=" + res.count())
    logger.traceExit()
    res
  }


  import org.nimajneb.kafka.DFUtils._

  def propertiesLikeMessage(df: DataFrame): DataFrame = {

    df.select("timeMillis", "markers", "message")
      .withColumn("time", from_unixtime($"timeMillis" / 1000))
      .where($"message".like("%=%") && $"markers".contains("SUPERVISION"))
      .withColumn("_tmp", split($"message", "="))
      .withColumn("properties", $"_tmp".getItem(0))
      .withColumn("value", $"_tmp".getItem(1))
      .drop("_tmp", "message", "timeMillis")
      .groupBy("time", "markers")
      .pivot("properties")
      .agg(concat_ws(",", collect_list($"value")).alias("values"))
    //.agg(count($"value"))

  }


  /** Transform nested struct marker
    *
    * The nested structure of Log4J markers may vary on each batch of data,
    * we recurse through the column structure extracting each marker.name
    *
    * @return
    */
  def recursiveExplodeMarkers(df: DataFrame): DataFrame = {


    val cols = df.columns
    val hasMarkersColumn = df.hasColumn("markers")
    val hasParents = df.hasColumn("marker.parents")
    val hasMarkerName = df.hasColumn("marker.name")
    logger.traceEntry()
    logger.debug(DKSup, "recursiveExplodeMarkers.in.hasMarkerName=" + hasParents)
    logger.debug(DKSup, "recursiveExplodeMarkers.in.hasParentCol=" + hasParents)
    logger.debug(DKSup, "recursiveExplodeMarkers.in.hasMarkersCol=" + hasMarkersColumn)
    logger.debug(DKSup, "recursiveExplodeMarkers.in.cols=" + cols.mkString(","))
    logger.debug(DKSup, "recursiveExplodeMarkers.in.count=" + df.count)

    val addMarkersCol = if (hasMarkersColumn) df else df.withColumn("markers", lit(null))
    //addMarkersCol.select("markers","marker").printSchema()


    if (hasParents) { // Condition to recurse

      val addMarker = addMarkersCol
        .withColumnRenamed("markers", "_markers")
        .withColumn("markers", when($"_markers".isNotNull,
          concat($"marker.name", lit(","), $"_markers"))
          .otherwise($"marker.name"))


      //addMarker.select("markers","marker","_markers").show(false)

      val x = addMarker.drop($"_markers")
        .withColumnRenamed("marker", "_subMarker")
        .withColumn("marker", explode_outer($"_subMarker.parents"))

      //x.select("marker","_subMarker","markers").show(false)

      logger.info(DKSup2, "recurse.markerStructure=" + df.select($"marker").schema)
      recursiveExplodeMarkers(x.drop("_subMarker"))


    } else { // End of the recursion condition
      logger.debug(DKSup, "end recursion")
      addMarkersCol
        .withColumnRenamed("markers", "_markers")
        .withColumn("markers", when($"marker.name".isNotNull, concat($"marker.name", lit(","), $"_markers")).otherwise($"_markers"))
        .drop("marker", "_markers")
    }
  }


  @Deprecated
  def explodeMarkers(df: DataFrame): DataFrame = {
    val res = df.withColumn("m1", $"marker.name")
      .withColumn("_m2", explode_outer($"marker.parents"))
      .withColumn("m2", $"_m2.name")
      //.withColumn("_m3", explode_outer($"_m2.parents"))
      //.withColumn("m3" , $"_m3.name")
      .drop("_m2") //, "_m3")

    res
  }

  @Deprecated
  def flattenMarkers(df: DataFrame): DataFrame = {
    explodeMarkers(df)
      .withColumn("markers", concat($"m1", lit(","), $"m2", lit(","), $"m3"))
      .drop("marker", "m1", "m2", "m3")
  }


}

