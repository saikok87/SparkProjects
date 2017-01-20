package com.scala.test

import Array._

object MaxOperationArray {
  def main(args: Array[String]){
    //var arr1: Array[Any] = new Array[Any](3)
    
    var arr1 = Array(1,"sai",29)
    var arr2 = Array(2,"Nilesh",27)
    var arr3 = Array(3,"vishal",26)
    
    val list1: List[Array[Any]] = List()
    
    var max: Int = 0
    
    list1.foreach { x => 
      println(x.mkString(",")) }
    
    
    list1.foreach { x => 
      //println("Value = " + map1(x).mkString(",")) 
      println("age= " + (x)(1).asInstanceOf[Int])
      
      val age: Int = x(1).asInstanceOf[Int]
      
      if(age > max) {
        max = age        
      }
    }
    
    println("Max Age is = " + max)
  }
}