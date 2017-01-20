package com.spark.sql

object SparkParqMergeTest {
  def main(args: Array[String]) {
    
    val sqlContext = new org.apache.spark.sql.SQLContext(SparkCon.sc)
    import sqlContext.implicits._
    
    // Create a simple DataFrame, stored into a partition directory
    val df1 = SparkCon.sc.makeRDD(1 to 5).map(i=>(i,i*2)).toDF("single","double")
    df1.write.parquet("PATH_TO_FILE")
    
    // Create another DataFrame in a new partition directory,
    //  adding a new column and dropping an existing column
    val df2 = SparkCon.sc.makeRDD(6 to 10).map(i=>(i,i*3)).toDF("single","triple")
    df2.write.parquet("PATH_TO_FILE")
    
    // Read the partitioned table
    val df3 = sqlContext.read.parquet("PATH_TO_FILE")
    df3.printSchema()
    df3.show()
    
  }
}