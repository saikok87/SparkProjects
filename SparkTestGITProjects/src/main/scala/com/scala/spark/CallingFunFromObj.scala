package com.scala.spark

import org.apache.spark.{SparkContext,SparkConf}

object Spark {
  // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Functions1"))
}

object CallingFunFromObj {
  def main(args: Array[String]) {
    new Test().doIT
  }
}

class Test {
  val rddList = Spark.sc.parallelize(List(1,2,3))
  
  def doIT() = {
    val after = rddList.map(somefun)
    after.collect().foreach(println)
  }
  
  val somefun = (a:Int) => a+1
}