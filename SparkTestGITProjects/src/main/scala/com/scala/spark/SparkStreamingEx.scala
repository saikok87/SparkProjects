/*package com.scala.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3

object SparkStreamingEx {
  def main(args:Array[String]){
    val conf = new SparkConf().setAppName("Spark Functions1")
    val ssc = new StreamingContext(conf,Seconds(1))
  }
}*/