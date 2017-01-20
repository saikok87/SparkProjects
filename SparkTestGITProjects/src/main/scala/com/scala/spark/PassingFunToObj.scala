package com.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object PassingFunToObj {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Functions e.g."))
    val lines = sc.textFile("PATH_TO_FILE")
    val after = lines.map(MyFunctions.fun1)
    after.saveAsTextFile("PATH_TO_FILE")   
  }
}

object MyFunctions {
  def fun1(s:String): String = {
   var str = (s)
   return str
  }
}