package io.github.chcyang.spark.streaming.job

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

object KafkaClient {

  def main(args: Array[String]): Unit = {


  }


  def getProducer(): Producer[String, String] = {

    val props: Properties = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.123:6667")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "100")

    new KafkaProducer[String, String](props)
  }

}
