package com.scala.test

object MapOperations {
  def main(args: Array[String]) {
    
    val colors = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")
    val nums: Map[Int, Int] = Map()
    
      println( "Keys in colors : " + colors.keys )
      println( "Values in colors : " + colors.values )
      println( "Check if colors is empty : " + colors.isEmpty )
      println( "Check if nums is empty : " + nums.isEmpty )
      
    val colors1 = Map("red" -> "#FF0000", "azure" -> "#F0FFFF", "peru" -> "#CD853F")
    val colors2 = Map("blue" -> "#0033FF", "yellow" -> "#FFFF00", "red" -> "#FF0000")
  
    var colorsCon = colors1 ++ colors2
    println( "colors1 ++ colors2 : " + colorsCon)
    
    //iterate
    colors.keys.foreach { x => 
      println("Key = " + x)
      println("Value = " + colors(x))
    }
      
     val capitals = Map("France" -> "Paris", "Japan" -> "Tokyo")
      
      println("capitals.get( \"France\" ) : " +  capitals.get( "France" ))
      println("capitals.get( \"India\" ) : " +  capitals.get( "India" ))
      
  }
}