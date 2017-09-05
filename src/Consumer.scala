package com.egen.pipelines.utils

import java.util

import com.egen.pipelines.ImageProcessor
import com.egen.pipelines.model.Message
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._
import java.util.Properties

class Consumer(imageProcessor: ImageProcessor, dataImageTopic: String, brokerHost: String) {

  val props = new Properties()
  props.put("bootstrap.servers", brokerHost )
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("group.id", "something")

  val consumer = new KafkaConsumer[String, String](props)

  def start() = {
    println("consumer starting.....")
    consumer.subscribe(util.Collections.singletonList(dataImageTopic))
    var count = 0
    while (true) {
      val records = consumer.poll(0)
      for (record <- records.asScala) {
        println("Processing record....." + record.value)
        val recordArray = record.value().split(" ")
        val message = Message(recordArray(0),
          recordArray(1),
          recordArray(2),
          recordArray(3),
          recordArray(4),
          recordArray(5))
        imageProcessor.processImage(message, count)
        count = count + 1
        imageProcessor.processText(message)
      }
    }
  }
}
