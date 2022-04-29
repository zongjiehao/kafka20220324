package com.haozj.kafka.patitioner

import java.time.Duration
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.JavaConverters._

object KakfaConsumerDemo {
    def main(args: Array[String]): Unit = {
        val properties = new Properties()
        properties.put("bootstrap.servers", "tencent:9092")
        properties.put("topic", "haozj")
        properties.put("group.id", "suibian1")
        //自动提交offset
        properties.put("enable.auto.commit", "true")
        //提交间隔
        properties.put("auto.commit.interval.ms", "1000")
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest")


        val kafkaConsumer = new KafkaConsumer[String, String](properties)
        val topic = List("haozj")
        kafkaConsumer.subscribe(topic.asJava)
        var flag = true
        while (flag) {
            val records: ConsumerRecords[String, String] = kafkaConsumer.poll(Duration.ofSeconds(2))
            val iter: util.Iterator[ConsumerRecord[String, String]] = records.iterator()
            while (iter.hasNext) {
                val record: ConsumerRecord[String, String] = iter.next()
                println("消费到" + record.topic() +
                        ":" + record.partition() +
                        ":" + record.offset() +
                        ":" + record.key() +
                        ":" + record.value())
//                if(record.value().contains("20")){
//                    flag=false
//                }

            }
        }
    }
}
