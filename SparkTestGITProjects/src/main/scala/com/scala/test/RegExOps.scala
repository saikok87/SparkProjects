package com.scala.test

import scala.util.matching.Regex

object RegExOps {
  def main(args: Array[String]){
    val pattern = "Scala".r
    val str = "Scala is scalable and cool"
    println(pattern findFirstIn str)
    
    val pat1 = new Regex("(S|s)cala")
    println((pat1 findAllIn str).mkString(","))
    
    println(pattern replaceFirstIn(str, "Java"))
    
    val pat2 = new Regex("abl[ae]\\d+")
    val str2 = "ablaw is able1 and cool"
    
    println((pat2 findAllIn str2).mkString(","))
  }
}