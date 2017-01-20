package com.scala.spark

object TakeSamplingEx {
  def main(args: Array[String]) {
    val inputrdd = SparkCon.sc.parallelize{Seq(10, 4, 5, 3, 11, 2, 6)}
    println("Sampled output::::: "+inputrdd.takeSample(false, 3, System.nanoTime().toInt).toString())
  }
}