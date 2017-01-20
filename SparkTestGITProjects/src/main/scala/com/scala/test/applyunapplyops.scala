package com.scala.test

object applyunapplyops {
  def main(args: Array[String]) {
      val x = applyunapplyops(5)
      println(x)

      x match {
         case applyunapplyops(num) => println(x+" is bigger two times than "+num)
         
         //unapply is invoked
         case _ => println("i cannot calculate")
      }
   }
   def apply(x: Int) = x*2
   def unapply(z: Int): Option[Int] = if (z%2==0) Some(z/2) else None
}