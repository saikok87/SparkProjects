package com.scala.spark

import org.apache.spark.{SparkContext,SparkConf}

object AccessingFields {
   def main(args: Array[String]) {
    new FieldTest().doIT
  }
}

//http://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou
class FieldTest extends java.io.Serializable {
  
  val rddList = SparkCon.sc.parallelize(Array(7,8,9))
  val str = "Hello"
  
  def doIT = {
      val after = rddList.map(somefun)
      after.collect().foreach(println)
  }
  
  def somefun(a:Int) = str + a
  
}