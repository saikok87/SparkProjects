package com.spark.stream

import org.apache.spark.{SparkContext,SparkConf}

object SparkCon {
  // create Spark context with Spark configuration
    val conf = new SparkConf().setAppName("Spark Streams")

}