package com.scala.test

object TupleOperations {
  def main(args: Array[String]) {
    val t = (4,3,2,1)
    val sum = t._1+t._2+t._3+t._4
    println( "Sum of elements: "  + sum )
    
    //iterate over
    t.productIterator.foreach { x => println("Value = "+x) }
    
    val t3 = new Tuple3(1,"hello",Console)
    println(t3.toString())
    
    val t2 = new Tuple2("Scala","Hello")
    println("Swapped Tuple: " + t2.swap)
    
   
    
  }
}