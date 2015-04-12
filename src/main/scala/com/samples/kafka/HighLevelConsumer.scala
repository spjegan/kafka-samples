package com.samples.kafka

import java.util.Properties

import akka.actor.{Actor, ActorSystem, Props}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.utils.Logging

/**
 * Created by jegan on 12/4/15.
 */
class HighLevelConsumer(val zookeeper: String, val group: String, val topic: String, val threadCount: Int) extends Logging {

  val props = new Properties()
  props.put("zookeeper.connect", zookeeper)
  props.put("group.id", group)
  props.put("zookeeper.session.timeout.ms", "400")
  props.put("zookeeper.sync.time.ms", "200")
  props.put("auto.commit.interval.ms", "1000")

  val consumerConnector = Consumer.create(new ConsumerConfig(props))
  val streamsMap = consumerConnector.createMessageStreams(Map(topic -> threadCount))

  val streams = streamsMap(topic)

  val as = ActorSystem("Kafka-Consumers")

  streams.foreach {
//    val reader = as.actorOf(Props[StreamReader], name = "StreamReader")
//    reader ! Consume(_)
    for (itr <- _) {
      Console.println(s"Message is ${itr.message()}")
    }
  }

  Thread.sleep(Long.MaxValue)
}

case class Consume(kafkaStream: KafkaStream[Array[Byte], Array[Byte]])





class StreamReader extends Actor {

  override def receive: Receive = {
    case Consume(ks) =>
      read(ks)
    case _ =>
      Console.err.println("Unknown Message")
  }

  def read(kafkaStream: KafkaStream[Array[Byte], Array[Byte]]): Unit = {
    for (itr <- kafkaStream) {
      Console.println(s"Message is ${itr.message()}")
    }
  }
}

object KafkaConsumerTest extends App {
  val consumer = new HighLevelConsumer("localhost:2181", "test-group1", "replication-test", 5)
}