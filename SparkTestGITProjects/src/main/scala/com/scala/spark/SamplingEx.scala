package com.scala.spark

object SamplingEx {
  def main(args: Array[String]) {
    
    val rawData = SparkCon.sc.textFile("PATH_TO_FILE")
    val sampledData = rawData.sample(false, 0.1, 1234)
    val sSize= sampledData.count()
    val rSize = rawData.count()
        
    println("rawData Size::::::"+rSize)
    println("sampleData Size::::::"+sSize)
    
  }
}