package io.github.chcyang.spark.streaming.job

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, Trigger}

class TestStructuredStreamingKafkaSource {


  def run(): Unit = {

    val spark = SparkSession.builder().master("local[*]")
      .appName(this.getClass.getCanonicalName)
      .getOrCreate()

    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.123:6667,192.168.1.124:6667,192.168.1.125:6667")
      .option("subscribe", "test-streaming-rand")
      .option("startOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 100)
      .load()


    import spark.implicits._
    val selectDf = df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
      .as[(String, String)]

    selectDf.writeStream
      .format("console")
      .outputMode(OutputMode.Append())
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
      .awaitTermination()


    spark.stop()


  }
}
