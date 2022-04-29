package com.haozj.kafka.patitioner

import java.util

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class MyPatition extends Partitioner{
    override def partition(topic: String, key: Any, keyBytes: Array[Byte], value: Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
        if(value.toString.contains("haozj")){
            0
        }else{
            1
        }
    }

    override def close(): Unit = {}

    override def configure(configs: util.Map[String, _]): Unit = {}
}
