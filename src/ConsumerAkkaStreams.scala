package com.egen.pipelines

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.egen.client.OCRClient
import com.egen.pipelines.model.Message
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}
import scala.concurrent._
import ExecutionContext.Implicits.global

import scala.concurrent.Future

class ConsumerAkkaStreams(imageProcessor: ImageProcessor) {

  val config = ConfigFactory.load()
  implicit val system = ActorSystem.create("akka-stream-kafka-getting-started", config)
  implicit val mat = ActorMaterializer()

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("jeroen-akka-stream-kafka-test")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

  val oCRClient = new OCRClient
  var count = 0
  def start() = {

    println("Multi Consumer starting...")
    Consumer.committableSource(consumerSettings, Subscriptions.topics("data-image02"))
      .map(msg => {
        //  println(msg.record.value)
        val recordArray = msg.record.value().split(" ")
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
      })
      .runWith(Sink.ignore)
  }
}
