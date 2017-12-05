package org.nimajneb.kafka

import java.util.UUID

import org.apache.logging.log4j.scala._
import org.apache.spark.sql.SparkSession


/** ZK & KAFKA
import sys.process._
"cmd"!
/home/ben/kafka_2.11-1.0.0/bin/zookeeper-server-start.sh /home/ben/kafka_2.11-1.0.0/config/zookeeper.properties > zk.log &
/home/ben/kafka_2.11-1.0.0/bin/kafka-server-start.sh /home/ben/kafka_2.11-1.0.0/config/server.properties > kafka.log &

// to check the kafka output
/home/ben/kafka_2.11-1.0.0/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
 */
object StructuredKafkaWordCount extends Logging{

  def main(args: Array[String]): Unit = {
    if (args.length < 0) {
      System.err.println("Usage: StructuredKafkaWordCount <bootstrap-servers> " +
        "<subscribe-type> <topics> [<checkpoint-location>]")
      System.exit(1)
    }

//    val Array(bootstrapServers, subscribeType, topics, _*) = args
    val checkpointLocation =
      if (args.length > 3) args(3) else "/tmp/temporary-" + UUID.randomUUID.toString


    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("StructuredKafkaWordCount")
      .getOrCreate()

    //val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


    import spark.implicits._

    // Create DataSet representing the stream of input lines from kafka
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "test")
      .load()
      .selectExpr( "CAST(value AS STRING)")//"topic","CAST(key AS STRING)",
      .as[String]


    // Generate running word count
    val wordCounts = lines.flatMap(_.trim().split(" ")).groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "/tmp/temporary-" + UUID.randomUUID.toString)
      .start()

    query.awaitTermination()


//    lines.foreachRDD { rdd =>
//
//      // Get the singleton instance of SparkSession
//      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
//      import spark.implicits._
//
//      // Convert RDD[String] to DataFrame
//      val wordsDataFrame = rdd.toDF("word")
//
//      // Create a temporary view
//      wordsDataFrame.createOrReplaceTempView("words")
//
//      // Do word count on DataFrame using SQL and print it
//      val wordCountsDataFrame =
//        spark.sql("select word, count(*) as total from words group by word")
//      wordCountsDataFrame.show()
//    }




  }
}