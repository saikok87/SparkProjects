package com.spark.usecases.examples

import com.spark.sql.SparkCon
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.tools.GetConf
import org.apache.hadoop.conf.Configuration

object MultiOutputTest {
  def main(args: Array[String]) {
    
    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(SparkCon.sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._
    
    val outputPath = "PATH_TO_FILE"    
    val conf: Configuration = new Configuration()
    val fs: FileSystem = FileSystem.get(conf)
    
    //delete output path if already exists
    if(fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath),true)
    }
    
    val people_rdd = SparkCon.sc.parallelize(Seq((1,"sai"),(2,"nilesh"),(3,"vishal")))
    val people_df = people_rdd.toDF("number","name")
    people_df.write.partitionBy("number").save(outputPath) //default parquet file will be generated(Spark 1.4.1)
    
  }
}