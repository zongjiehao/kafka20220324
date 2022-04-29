package com.haozj.kafka.producer

import java.util.Properties

import com.haozj.kafka.util.DateUtils
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object KafkaProducerDemo1 {
    def main(args: Array[String]): Unit = {
        /**
          * 异步发送，带回调
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

        for(i<-1 to 10){
            //指定topic和value
            kafka.send(new ProducerRecord[String,String]("haozj","kakfa@@@"+ i),
                new Callback {
                    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                        if(exception !=null){
                            println("消息发送失败"+exception.getMessage)
                        }else{
                            println("消息发送成功:"+DateUtils.tsToDt(metadata.timestamp())+"--"+metadata.topic()+"--"+
                            metadata.partition()+"--"+metadata.offset())
                        }
                    }
                })

        }
        kafka.close()
    }
}
