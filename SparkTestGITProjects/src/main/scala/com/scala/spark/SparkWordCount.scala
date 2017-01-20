package com.scala.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SparkWordCount {
  
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("Spark Word Count"))
    
    val textFile = sc.textFile("PATH_TO_FILE")
    
    val counts = textFile.flatMap(line => line.split(","))
                                 .map(word => (word,1))
                                 .reduceByKey(_+_)
    //counts.saveAsTextFile("PATH_TO_FILE") // saves o/p in multiple part files under this dir
    
 //thorwing exception, compatibility issue   //counts.coalesce(1).saveAsTextFile("PATH_TO_FILE") // use coalesce method to save o/p into a single file
    
                                 
                                 
  }
}