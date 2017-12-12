package com.nimajneb.kafka;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.*;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import static org.apache.spark.sql.functions.*;

public class J8KafkaConsumer {


    public static void main(String... args){
        String brokers = args[0];
        String topics = args[1];

        SparkSession spark = SparkSession
                .builder()
                .appName("Simple Application")
                .master("local[*]")
                .getOrCreate();

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        StructType logSchema = JsonLogSchema.struct();

        Dataset<Row> kafka = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", brokers)
                .option("subscribe", topics)
                .load();

        Dataset<Row> parsed = kafka
                .select(from_json(col("value").cast("string"), logSchema).alias("parsed_value"))
                .select("parsed_value.*");

        Dataset<Row> tenSecondCounts = parsed
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                        col("key"),
                        window(col("timestamp"), "1 day"))
                .count();

        StreamingQuery query = tenSecondCounts
                .writeStream()
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .outputMode("append")
                .format("console")
                .option("truncate", false)
                .start();

        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
    }
}
