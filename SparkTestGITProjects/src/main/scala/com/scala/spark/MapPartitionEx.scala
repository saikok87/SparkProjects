package com.scala.spark

// http://apachesparkbook.blogspot.in/2015/11/mappartition-example.html

object MapPartitionEx {
  def main(args: Array[String]) {
    val rdd1 = SparkCon.sc.parallelize(List("yellow","red","blue","cyan","black"), 3)
    
    val mapped = rdd1.mapPartitionsWithIndex {
      // 'index' represents the Partition No
      // 'iterator' to iterate through all elements
      // in the partition
      
      (index, iterator) => {
        println("Called in Partition -> " + index)
        val myList = iterator.toList
        // In a normal user case, we will do the
        // the initialization(ex : initializing database)
        // before iterating through each element
        myList.map (x => x + " -> " + index).iterator
      }
    }
    mapped.collect().foreach(println) // to print o/p on console
  }
}