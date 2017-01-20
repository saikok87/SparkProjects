package com.scala.test

object MaxOperation {
  def main(args: Array[String]) {
    var map1:Map[Int, Array[Any]] = Map()
    
    map1+=(1->Array("sai",28))
    map1+=(2->Array("nilesh",26))
    map1+=(3->Array("vishal",27))
    
    println(map1.keys)
    
    var max: Int = 0
    
    map1.keys.foreach { x => 
      //println("Value = " + map1(x).mkString(",")) 
      println("age= " + map1(x)(1))
      
      val age: Int = map1(x)(1).asInstanceOf[Int]
      
      if(age > max) {
        max = age        
      }
    }
    
    println("Max Age is = " + max)
  }
}