package com.scala.test

trait Printable extends Any {
  def print(): Unit = println(this)
}

class Wrapper(val underlying: Int) extends AnyVal with Printable

object Demo2 {
  def main(args: Array[String]) {
    val w = new Wrapper(4)
    w.print()
  }
}