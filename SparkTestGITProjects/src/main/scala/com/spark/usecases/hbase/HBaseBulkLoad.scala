package com.spark.usecases.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

object HBaseBulkLoad {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Hbase"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val tableName = "hbase_table"
    val cols = "id,name"
    val col_fam = "info"
    var col_Arr: Array[String] = cols.split(",") 
    
    val inputData = sc.textFile("PATH_TO_FILE")
                      .map(line => line.split(","))
                      .map(a=>Array(a(0),a(1)))
    
    inputData.foreachPartition { iter => 
        val conf = HBaseConfiguration.create()
        conf.set("mapreduce.job.queuename", "queue")
        conf.set(TableInputFormat.INPUT_TABLE, tableName)
        val hTable = new HTable(conf,tableName)
        
        iter.foreach { a => 
           var put = new Put(a(0).getBytes())
           put.add(col_fam.getBytes(), col_Arr(1).getBytes(), a(1).getBytes)
           hTable.put(put)
        }
    }
   
  }
}