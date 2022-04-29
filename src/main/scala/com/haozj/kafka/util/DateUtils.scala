package com.haozj.kafka.util

import java.sql.Timestamp
import java.text.SimpleDateFormat

object DateUtils {
    def tsToDt(tmp:Long) ={
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        format.format(tmp)
    }

    def main(args: Array[String]): Unit = {
        val a = 1648139470835L
        println(tsToDt(a))
    }
}
