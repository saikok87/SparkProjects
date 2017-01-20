package com.scala.test

import java.io.PrintWriter
import java.io.File
import scala.io.Source

object FileOps {
  def main(args: Array[String]) {
    
    //write to file
    val writer = new PrintWriter(new File("D:\\test.txt"))
    writer.write("Hello Scala!!")
    writer.close
    
    //read from console
    println("Input::: ")
    val line = Console.readLine()
    println("you just typed:: "+line)
    
    //read from file
    println("reading from file....")
    Source.fromFile("D:\\test.txt").foreach { 
      print
    }
  }
}