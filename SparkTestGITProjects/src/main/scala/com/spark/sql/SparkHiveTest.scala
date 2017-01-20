package com.spark.sql

object SparkHiveTest {
  def main(args: Array[String]) {
  
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(SparkCon.sc)
    
    sqlContext.sql("SET spark.yarn.dist.files='/../spark/spark-1.4.1/conf/hive-site.xml'")
    sqlContext.sql("SET hive.metastore.warehouse.dir='/../warehouse'");
    
    /*sqlContext.sql("CREATE DATABASE IF NOT EXISTS Spark_Hive_DB")
    sqlContext.sql("")
    sqlContext.sql("CREATE TABLE IF NOT EXISTS Spark_Hive_DB.hive_test_table (key INT, value STRING)")
    sqlContext.sql(" LOAD DATA INPATH 'PATH_TO_FILE'")
    */
    sqlContext.sql("FROM Spark_Hive_DB.hive_test_table SELECT key, value").collect().foreach(println)
  
  }
}