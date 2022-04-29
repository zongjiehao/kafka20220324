package com.haozj.kafka.interceptor

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

/**
  * 将时间戳添加到消息的前面
  */
class TimeStampInterceptor extends ProducerInterceptor[String,String]{
    override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
        val value: String = record.value()
        val result: String = System.currentTimeMillis()+"->" +value
        val newRecord = new ProducerRecord[String,String](record.topic(),record.partition(),record.key(),result)
        newRecord
    }

    override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {

    }

    override def close(): Unit = {

    }

    override def configure(configs: util.Map[String, _]): Unit = {

    }
}
