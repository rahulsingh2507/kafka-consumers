package com.egen.pipelines.utils

import java.util.Properties

import com.egen.pipelines.ImageProcessor
import com.egen.pipelines.model.Message
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, _}
import scala.language.implicitConversions

class StreamsConsumer(imageProcessor: ImageProcessor, brokerHost: String, dataImageTopic: String) {

  val config = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerHost)
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
    properties
  }

  var count = 0

  def start = {
    println("Consumer starting...")
    val builder = new KStreamBuilder
    val textLines: KStream[String, String] = builder.stream(dataImageTopic)
    textLines.foreach((key, value) => {
      val recordArray = value.split(" ")
      val message = Message(recordArray(0),
        recordArray(1),
        recordArray(2),
        recordArray(3),
        recordArray(4),
        recordArray(5))
      println(message)
      val processedImage = Future {
        imageProcessor.processImage(message, count)
      }
      count = count + 1
      val processText = Future {
        imageProcessor.processText(message)
      }
      println(processText)
    })


    val streams = new KafkaStreams(builder, config)
    streams.start()
  }
}

