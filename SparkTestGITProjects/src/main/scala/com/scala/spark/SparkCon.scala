package com.scala.spark

import org.apache.spark.{SparkContext,SparkConf}

object SparkCon {
  // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Functions1"))

}