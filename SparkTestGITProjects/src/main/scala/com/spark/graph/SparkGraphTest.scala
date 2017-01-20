package com.spark.graph

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._

object SparkGraphTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Graph ops"))
    // Create an RDD for the vertices
    val users: RDD[(VertexId,(String,String))] =
      sc.parallelize(Array((3L,("sai","student")), (7L,("vishal","postdoc")), 
                           (5L,("kuwar","prof")), (2L,("archana","prof")),
                           (4L,("peter","student"))))
    
    // Create an RDD for edges
    val relationships: RDD[Edge[String]] =
      sc.parallelize(Array(Edge(3L,7L,"collab"), Edge(5L,3L,"advisor"),
                           Edge(2L,5L,"colleague"), Edge(5L,7L,"pi"),
                           Edge(4L,0L,"student"), Edge(5L,0L,"colleague")))
    
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John","Missing")
    
    // Build the initial Graph
    val graph = Graph(users, relationships, defaultUser)
    
    // Count all users which are prof
    println("prof count::::::::::::: " + graph.vertices.filter {case(id,(name,pos)) => pos == "prof"}.count)
    // Count all the edges where src > dst
    println("edges count where src>dest::::: " + graph.edges.filter {e => e.srcId>e.dstId}.count)
    
    // Use the triplets view to create an RDD of facts.
    
    // Notice that there is a user 0 (for which we have no information) connected to users
   // 4 (peter) and 5 (kuwar).
    val facts: RDD[String] =
      graph.triplets.map(triplet => 
        triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1)
    facts.collect().foreach(println)
    println("###################################")
       
    // Remove missing vertices as well as the edges to connected to them
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    
    // The valid subgraph will disconnect users 4 and 5 by removing user 0
    validGraph.vertices.collect().foreach(println(_))
    
    println("###################################")
    validGraph.triplets.map(
      triplet => triplet.srcAttr._1 + " is the " + triplet.attr + " of " + triplet.dstAttr._1  
    ).collect().foreach(println(_))
    
    println("###################################")
    // Run Connected Components
    val ccGraph = graph.connectedComponents() // No longer contains missing field
    ccGraph.vertices.collect().foreach(println(_))
    
    // Restrict the answer to the valid subgraph
    val validCCGraph = ccGraph.mask(validGraph) 
    println("###################################")
    validCCGraph.vertices.collect().foreach(println(_))
    
  }
}