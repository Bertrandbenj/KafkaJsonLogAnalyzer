package com.nimajneb.kafka

import org.apache.spark.sql.types._

object JsonLogSchema {

  val struct =
    StructType(
      StructField("level", StringType, true) ::
        StructField("marker", StructType(
          StructField("name", StringType, true) ::
            StructField("parents", ArrayType(
              StructType(
                StructField("name", StringType, true) ::
                  StructField("parents", ArrayType(
                    StructType(
                      StructField("name", StringType, true) :: Nil
                    )
                  ), true) :: Nil
              )
            ), true) :: Nil
        ), true) ::
        StructField("timeMillis", LongType, false) ::
        StructField("message", StringType, false) :: Nil)




}