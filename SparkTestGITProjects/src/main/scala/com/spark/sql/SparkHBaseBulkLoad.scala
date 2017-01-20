package com.spark.sql

import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

object SparkHBaseBulkLoad {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Hbase"))
    val conf = HBaseConfiguration.create()
    val tableName = "hbase_table"
    val outputPath = "PATH_TO_FILE"
      
    val hTable = new HTable(conf,tableName)
    
    conf.set("mapreduce.job.queuename", "queue_name")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    
    val fs: FileSystem = FileSystem.get(conf)
    //delete output path if already exists
    if(fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath),true)
    }
    
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.getConfiguration.set("mapred.output.dir", outputPath)
    
    HFileOutputFormat.configureIncrementalLoad(job, hTable)
    
    //generate 10 sample data
    val num = sc.parallelize(1 to 10)
    val rdd = num.map(x=> {
        val kv: KeyValue = new KeyValue(Bytes.toBytes(x), "cf1".getBytes, "col1".getBytes, "value_x".getBytes)
        (new ImmutableBytesWritable(Bytes.toBytes(x)),kv)
    })
    
    // Directly bulk load to Hbase/MapRDB tables.
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    
    println("#################                      MapR-DB/HBase BulkLoad successful                         #########################")
  
  }
}