package io.github.chcyang.spark.streaming.kakfa

import io.github.chcyang.spark.streaming.job.KafkaClient
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}

import scala.util.Random

object KafkaProducerTest {

  def main(args: Array[String]): Unit = {

    val kafkaProducer: Producer[String, String] = new KafkaClient().getProducer()
    val random = new Random()

    for (i <- 1 to 100) {
      val key = random.nextInt().toString
      val value = random.alphanumeric.take(10).mkString
      val record: ProducerRecord[String, String] = new ProducerRecord("test-streaming-rand", key, value)

      kafkaProducer.send(record, new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = println(s"send complete $i")
      })
    }

    kafkaProducer.flush()


  }

}
