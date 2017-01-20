package com.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkMR {
  def main(args: Array[String]) {
    
    //count chars of lines
    val sc = new SparkContext(new SparkConf().setAppName("Spark MR"))
    val lines = sc.textFile("PATH_TO_FILE")
    val linesLength = lines.map(s=>s.length())
    val totalLength = linesLength.reduce((a,b)=>a+b)
    val intRdd = sc.parallelize(Seq(totalLength))
    intRdd.saveAsTextFile("PATH_TO_FILE")
    
  }
}