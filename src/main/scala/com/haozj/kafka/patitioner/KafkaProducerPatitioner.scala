package com.haozj.kafka.patitioner

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object KafkaProducerPatitioner {
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
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.haozj.kafka.patitioner.MyPatition")
        val kafka = new KafkaProducer[String,String](properties)

        for(i<-1 to 10){
            //指定topic和value
            var value=""
            if(i%2==0){
                value="haozj==="+i
            }
            else{
                value="wangtao==="+i
            }
            kafka.send(new ProducerRecord[String,String]("haozj",value))
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
