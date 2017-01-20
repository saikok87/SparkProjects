package com.scala.test

object PatternMatchOps {
  def main(args: Array[String]) {
    println(matchTest(2))
    println(matchTest("two"))
    println(matchTest("test"))
  }
  
  def matchTest(x:Any): Any = x match {
    case 1 => "One"
    case "two" => 2
    case y:Int => "scala.Int"
    case _ => "Many"  
  }
}