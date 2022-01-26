package io.github.chcyang.spark.streaming.kakfa

import java.time.Duration

import io.github.chcyang.spark.streaming.job.KafkaClient
import org.apache.kafka.clients.consumer.ConsumerRecord


object KafkaConsumerTest {

  def main(args: Array[String]): Unit = {

    val topic = "test-streaming-rand"
    val groupId = "test-rand"
    val kafkaConsumer = new KafkaClient().getConsumer(groupId, List(topic))

    kafkaConsumer.poll(Duration.ofSeconds(30))
      .records(topic)
      .forEach((t: ConsumerRecord[String, String]) => println(s"key =${t.key} value =${t.value()} timestamp =${t.timestamp()} "))
  }

}
