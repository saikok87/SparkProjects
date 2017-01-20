package com.spark.stream

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object StreamTest {
  def main(args: Array[String]) {
    val ssc=new StreamingContext(SparkCon.conf, Seconds(1))
    
    // Create a DStream that will connect to hostname:port, like localhost:9999
    val lines = ssc.socketTextStream("server", 9999)
    // Split each line into words
    val words = lines.flatMap(_.split(" "))
    // Count each word in each batch
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    
    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    
    ssc.start() //Start the computation
    ssc.awaitTermination() //Wait for the computation to terminate
    
  }
}