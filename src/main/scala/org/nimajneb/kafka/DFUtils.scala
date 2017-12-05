package org.nimajneb.kafka

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.Try

object DFUtils {

  implicit class DFImprovements(val df: DataFrame) {

    import df.sparkSession.implicits._

    def hasColumn(path: String) = Try(df(path)).isSuccess

    def asTime(): DataFrame = {
      df.withColumn("time", from_unixtime($"timeMillis" / 1000))
        .drop("timeMillis")
    }

    /**
      * |-- source: struct (nullable = true)
      * |    |-- class: string (nullable = true)
      * |    |-- file: string (nullable = true)
      * |    |-- line: long (nullable = true)
      * |    |-- method: string (nullable = true)
      *
      * @return df |-- src: string (nullable = true)
      */
    def sourceToString(): DataFrame = {
      df.withColumn("src", concat($"source.class", $"source.method", lit(":"), $"source.line"))
        .drop("source")

    }


    def propertiesLikeMessage(): DataFrame = {
      df.select("time", "markers", "message")
        .where($"message".like("%=%") && $"markers".contains("SUPERVISION"))
        .withColumn("_tmp", split($"message", "="))
        .withColumn("properties", $"_tmp".getItem(0))
        .withColumn("value", $"_tmp".getItem(1))
        .drop("_tmp", "message")
        .groupBy("time", "markers")
        .pivot("properties")
        .agg(concat_ws(",", collect_list($"value")).alias("values"))
      //.agg(count($"value"))

    }

    def clean(): DataFrame = {
      df.drop("contextMap", "endOfBatch", "loggerFqcn", "threadId", "threadPriority")
        .asTime()
        .sourceToString()
    }

    def toExcel(path: String): Unit = {
      df.write
        .format("com.crealytics.spark.excel")
        .option("sheetName", "Markers")
        .option("useHeader", "true")
        .option("dateFormat", "yy-mmm-d") // Optional, default: yy-m-d h:mm
        .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss.000
        .mode("overwrite")
        .save(path)
    }


  }

}