
package com.scala.test

object SetOperations {
  def main(args: Array[String]) {
      val fruit1 = Set("apples", "oranges", "pears")
      val fruit2 = Set("mangoes", "banana")

      // use two or more sets with ++ as operator
      var fruit = fruit1 ++ fruit2
      println( "fruit1 ++ fruit2 : " + fruit )

      // use two sets with ++ as method
      fruit = fruit1.++(fruit2)
      println( "fruit1.++(fruit2) : " + fruit )
   
      val num1 = Set(5,6,9,20,30,45)
      val num2 = Set(50,60,9,20,35,55)
      
      println("num1.&num2 = " + num1.&(num2))
      
  }
}