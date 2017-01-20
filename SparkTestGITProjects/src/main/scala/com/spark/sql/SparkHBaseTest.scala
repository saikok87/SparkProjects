package com.spark.sql

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.HTable;

object SparkHBaseTest {
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Hbase"))
    val conf = HBaseConfiguration.create()
    val tableName = "habse_table"
    conf.set("mapreduce.job.queuename", "queue")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    
    val hTable = new HTable(conf,tableName)
    var put = new Put(new String("row1").getBytes);
    put.add("cf1".getBytes(), "col1".getBytes(), "value1".getBytes())
    hTable.put(put)
    hTable.flushCommits()
  }
}