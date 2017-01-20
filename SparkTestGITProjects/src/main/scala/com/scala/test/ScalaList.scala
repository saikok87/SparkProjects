package com.scala.test

object ScalaList {
  def main(args: Array[String]) {
    val builder = List.newBuilder[Int]
builder += 10
builder += 11
builder += 12
val numbers3 = builder.result()
println(numbers3)
  }
}