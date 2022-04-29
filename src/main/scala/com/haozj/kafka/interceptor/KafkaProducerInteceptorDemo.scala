package com.haozj.kafka.interceptor

import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducerInteceptorDemo {
    def main(args: Array[String]): Unit = {
        /**
          * 异步发送，不带回调
          */
        val properties = new Properties()
        properties.put("bootstrap.servers", "tencent:9092")
        properties.put("topic","haozj")
        properties.put("acks", "all")
        //重试次数
        properties.put("retries", "5")
        //批次大小
        properties.put("batch.size", "16384")
        //等待时间
        properties.put("linger.ms", "1")
        //RecordAccumulator缓冲区大小 32M
        properties.put("buffer.memory", "33554432")
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val intercetors = List("com.haozj.kafka.interceptor.TimeStampInterceptor","com.haozj.kafka.interceptor.CountInterceptor")
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,intercetors.asJava)
        val kafka = new KafkaProducer[String,String](properties)

        for(i<-1 to 20){
            //指定topic和value
            kafka.send(new ProducerRecord[String,String]("haozj","kakfainterceptor"+ i))

        }
        kafka.close()
    }
}
