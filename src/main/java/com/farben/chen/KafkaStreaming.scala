package com.farben.chen

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.tools.nsc.interpreter.NamedParam.tuple


object KafkaStreaming {
  def main(args: Array[String]): Unit = {
    // offset保存路径



    val conf = new SparkConf()
      .setAppName("ScalaKafkaStream")
      //.setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(3))

    val bootstrapServers = "10.18.20.34:9092,10.18.20.86:9092,10.18.20.63:9092"
    val groupId = "streaming"
    val topicName = "spark-streaming"
    val maxPoll = 20000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))

    kafkaTopicDS.map(_.value)
      .flatMap(_.split(" "))
      .map(x => (x, 1L))
      .reduceByKey(_ + _)
      .transform(data => {
        val sortData = data.sortBy(_._2, false)
        sortData
      })
      .print()

    ssc.start()
    ssc.awaitTermination()

  }

}
