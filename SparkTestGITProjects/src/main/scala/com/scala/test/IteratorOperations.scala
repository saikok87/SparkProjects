package com.scala.test

object IteratorOperations {
  def main(args: Array[String]) {
    val it = Iterator("a", "number", "of", "words")
    
    while(it.hasNext) {
      println(it.next())
    }
  }
}