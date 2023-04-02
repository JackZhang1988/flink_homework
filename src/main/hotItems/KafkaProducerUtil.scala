package com.zjc.hotitems_analysis

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util.Properties

object KafkaProducerUtil {
  def main(args: Array[String]): Unit = {
    write2KafkaWithTopic("hotItems")
  }
  def write2KafkaWithTopic(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop103:9092")
    properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 创建kafka生产者
    val kafkaProducer = new KafkaProducer[String, String](properties)
    //从文件中读取数据，逐条发送
    val csvSource = io.Source.fromFile("C:\\Users\\jianbozhang\\projects\\bigData\\projects\\resources\\UserBehavior.csv")
    for (line <- csvSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      kafkaProducer.send(record)
    }
  }

}

