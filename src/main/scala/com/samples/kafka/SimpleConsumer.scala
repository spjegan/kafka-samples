package com.samples.kafka

import java.util

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.ProducerConfig

/**
 * Created by jegan on 11/4/15.
 */
class SimpleConsumer(val brokers: String, val consumerGroup: String, val topic: String, val fromBeginning: Boolean = true) {

  val consumerConfig = new util.HashMap[String, Object]()
//  consumerConfig.put("metadata.broker.list", brokers)
  consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  consumerConfig.put("group.id", consumerGroup)

  val consumer = new KafkaConsumer[String, String](consumerConfig)
  consumer.subscribe("replication-test")

  def process(records: util.Map[String, ConsumerRecords[String, String]]) = {


  }

  while(true) {
    val records = consumer.poll(100)
    System.out.println(s"Records are $records")
//    process(records)
  }
}

object ConsumerTest extends App {
  val brokers = "localhost:9091,localhost:9092,localhost:9093,localhost:9094"

  new SimpleConsumer(brokers, "test-group", "replication-test")
}
