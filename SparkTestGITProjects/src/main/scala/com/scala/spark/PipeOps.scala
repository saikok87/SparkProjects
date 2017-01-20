package com.scala.spark

import org.apache.spark.SparkFiles

object PipeOps {
  def main(args: Array[String]) {
    val data = List("hi","hello","how","are","you")
    val dataRDD = SparkCon.sc.makeRDD(data) //sc is SparkContext
    
    val scriptPath = "PATH_TO_FILE"
    SparkCon.sc.addFile(scriptPath,true)
    val pipeRDD = dataRDD.pipe(SparkFiles.get(scriptPath))
    pipeRDD.collect().foreach(println)
    
    val accum = SparkCon.sc.accumulator(0,"My Accumulator")
    
  }
}