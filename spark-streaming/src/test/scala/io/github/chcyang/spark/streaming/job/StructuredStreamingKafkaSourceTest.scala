package io.github.chcyang.spark.streaming.job

object StructuredStreamingKafkaSourceTest {

  def main(args: Array[String]): Unit = {

    new TestStructuredStreamingKafkaSource().run()
  }

}
