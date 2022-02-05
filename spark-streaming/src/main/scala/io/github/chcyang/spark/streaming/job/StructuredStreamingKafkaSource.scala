package io.github.chcyang.spark.streaming.job

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

class StructuredStreamingKafkaSource {


  def run(): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.123:6667,192.168.1.124:6667,192.168.1.125:6667")
      .option("subscribe", "test-streaming-rand")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 10)
      .load()


    import spark.implicits._
    val selectDf = df.selectExpr("CAST(key AS STRING) as key",
      "CAST(value AS STRING) as value",
      "CAST(timestamp AS LONG) as timestamp"
    )
      .as[(String, String, Long)]

    import org.apache.spark.sql.functions._
    selectDf
      .map {
        x =>
          println(s"key=${x._1},value=${x._2},timestamp=${x._3}")
          x
      }
      .toDF("key", "value", "timestamp")
      .withColumn("timestamp", to_timestamp(col("timestamp")))
      .withWatermark("timestamp", "1 hours")
      .groupBy(
        window($"timestamp", "15 minutes"),
        $"value")
      .count()

      .writeStream
      .format("console")
      .option("checkpointLocation", "/Users/neco/appsource/spark-test/checkpoint")
      .outputMode(OutputMode.Update())
      .trigger(Trigger.ProcessingTime(30, TimeUnit.SECONDS))
      .start()
      .awaitTermination()

    spark.stop()


  }
}
