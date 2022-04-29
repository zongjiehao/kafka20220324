package com.haozj.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducerDemo {
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
        val kafka = new KafkaProducer[String,String](properties)

        for(i<-1 to 20){
            //指定topic和value
            kafka.send(new ProducerRecord[String,String]("haozj","kakfa@@@"+ i))
            //指定分区
//            if(i%2==0){
//                kafka.send(new ProducerRecord[String,String]("haozj",0,null,"kakfa0"+ i))
//            }
//            else{
//                kafka.send(new ProducerRecord[String,String]("haozj",1,null,"kakfa0"+ i))
//            }
            //指定key
            //kafka.send(new ProducerRecord[String,String]("haozj",i.toString,"kakfa--"+ i))
        }
        kafka.close()
    }
}
