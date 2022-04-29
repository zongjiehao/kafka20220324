package com.haozj.kafka.producer

object aa {
    def main(args: Array[String]): Unit = {
        for(i<-1 to 10){
            println(i.toString.hashCode()+"----"+i.toString.hashCode()%2)
        }
    }
}
