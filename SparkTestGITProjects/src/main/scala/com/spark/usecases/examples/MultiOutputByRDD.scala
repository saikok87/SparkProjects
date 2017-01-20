package com.spark.usecases.examples

import com.spark.sql.SparkCon
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner

object MultiOutputByRDD {
  def main(args: Array[String]) {
    SparkCon.sc.textFile("PATH_TO_FILE")
        .map(line=>line.split(","))
        .map(a=>Array(a(0),a(1)))
       
  }
}