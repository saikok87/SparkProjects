package com.spark.sql

object SparkMySQLTest {
  def main(args: Array[String]) {
    val sqlContext = new org.apache.spark.sql.SQLContext(SparkCon.sc)
    
    val df_mysql = sqlContext.read.format("jdbc").option("url", "jdbc:mysql://server:3306/ooziemapr").option("driver", "com.mysql.jdbc.Driver").
                   option("dbtable", "db_name").option("user", "user").option("password", "pwd").load()
                   
    df_mysql.show() //show schema of table
    
    df_mysql.registerTempTable("oozie")
    
    df_mysql.sqlContext.sql("SELECT * FROM oozie LIMIT 10").collect().foreach(println)
    
  }
}