package com.haozj.kafka.interceptor

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

/**
  * 该拦截器统计发送和失败的消息个数
  *
  */
class CountInterceptor extends ProducerInterceptor[String, String] {
    private var success: Int = _
    private var fail: Int = _

    override def onSend(record: ProducerRecord[String, String]): ProducerRecord[String, String] = {
        record
    }

    override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception != null) {
            fail += 1
        } else {
            success += 1
        }
    }

    override def close(): Unit = {
        println("SUCCESS:" + success)
        println("FAIL:" + fail)
    }

    override def configure(configs: util.Map[String, _]): Unit = {}
}
