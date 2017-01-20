package com.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.FileOutputStream

object SimpleApp {
  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("Simple Application"))
    val logData = sc.textFile("PATH_TO_FILE", 2).cache()
    
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    
   // println(s"Lines with a: $numAs, Lines with b: $numBs")
    
    val x = sc.parallelize(Array(numAs,numBs))
    x.saveAsTextFile("PATH_TO_FILE")
    
//    val writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("PATH_TO_FILE")))
//    
//    writer.write(numAs.toInt + "\n")
//    writer.write(numBs.toInt)
//    writer.close()
    sc.stop()
    
  }
}