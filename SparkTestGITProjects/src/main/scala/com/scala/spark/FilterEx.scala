package com.scala.spark

object FilterEx {
  def main(args: Array[String]) {
    val x = SparkCon.sc.parallelize(1 to 10, 2)
    
    //filter operation
    val y = x.filter(_%2==0)
    //y.collect().foreach(println) // to print filtered results to console
    y.saveAsObjectFile("PATH_TO_FILE") //to save result as object file
    
  }
}