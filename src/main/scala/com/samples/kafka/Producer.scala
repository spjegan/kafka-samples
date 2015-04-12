package com.samples.kafka

import java.util

import org.apache.kafka.clients.producer._

/**
 * Created by jegan on 11/4/15.
 */
object Producer extends App {

  val config = new util.HashMap[String, Object]()
  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091,localhost:9092,localhost:9093,localhost:9094")
  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  config.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer")

  val producer = new KafkaProducer[String, String](config)

  val callback = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      Console.println(s"Offset is ${metadata.offset()}")
      Console.println(s"Partition is ${metadata.partition()}")
    }
  }

  producer.send(new ProducerRecord[String, String]("replication-test", "test", "Hello!"), callback)
  producer.send(new ProducerRecord[String, String]("replication-test", "test", "How are you???"), callback)

  producer.close()
}