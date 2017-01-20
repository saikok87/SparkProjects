package com.spark.sql

object SparkSQLSchemaTest {
  def main(args: Array[String]) {
    val sqlContext = new org.apache.spark.sql.SQLContext(SparkCon.sc)
    
    //create RDD
    val people = SparkCon.sc.textFile("PATH_TO_FILE")
    // The schema is encoded in a string
    val schemaString = "email,mobile,name,surname"
    
    // Import Row.
    import org.apache.spark.sql.Row;
    // Import Spark SQL data types
    import org.apache.spark.sql.types.{StructType,StructField,StringType};
    // Generate the schema based on the string of schema
    val schema = StructType(schemaString.split(",").map(fieldName => StructField(fieldName, StringType, true)))
    // Convert records of the RDD (people) to Rows.
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0),p(1),p(2),p(3)))
    // Apply the schema to the RDD.
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    // Register the DataFrames as a table.
    peopleDataFrame.registerTempTable("people")
    
    // SQL statements can be run by using the sql methods provided by sqlContext.
    val results = sqlContext.sql("SELECT name, surname FROM people")
    
    // The results of SQL queries are DataFrames and support all the normal RDD operations.
    // The columns of a row in the result can be accessed by field index or by field name.
    
    results.map(t => "Name::: " + t(0) + " SurName::: " + t(1)).collect().foreach(println)
    
  }
}