package com.samples.kafka

import java.util

import org.apache.kafka.clients.producer._

/**
 * Created by jegan on 11/4/15.
 */
class Producer(brokers: String, topic: String) {

  val config = new util.HashMap[String, Object]()
  config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  config.put(ProducerConfig.CLIENT_ID_CONFIG, "test-producer")
  config.put(ProducerConfig.ACKS_CONFIG, "1")

  val producer = new KafkaProducer[String, String](config)

  def send(message: String) = producer.send(new ProducerRecord[String, String](topic, message))

  def send(message: String, callback:() => Callback) = producer.send(new ProducerRecord[String, String](topic, message), callback())

  def shutdown = producer.close()
}

object ProducerTest extends App {
  val producer = new Producer("localhost:9091,localhost:9092,localhost:9093,localhost:9094", "replication-test")

  def callback() = new Callback {
    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
      Console.println(s"Offset is ${metadata.offset()}")
      Console.println(s"Partition is ${metadata.partition()}")
    }
  }

  producer.send("Hi...")
  producer.send("Hello Friend...", callback)
  producer.shutdown
}