package com.spark.usecases.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

object HBaseExtract {
  final val tableName = "hbase_table"
  final val outputPath = "PATH_TO_FILE"
  final val cols = "id,name"
  final val col_fam = "info"
  final val col_Arr: Array[String] = cols.split(",") 
  
case class PInfo(rowKey: String, name: String)
  
  object PInfo extends Serializable {
    def parsePInfo(result: Result): PInfo = {
      val rowKey = Bytes.toString(result.getRow())
      val p0 = rowKey.split(" ")(0)
      val p1 = Bytes.toString(result.getValue(col_fam.getBytes(), col_Arr(1).getBytes()))
      PInfo(p0,p1)
    }
  }
  
  def main(args: Array[String]){
    val sc = new SparkContext(new SparkConf().setAppName("Spark Hbase"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val conf = HBaseConfiguration.create()
    conf.set("mapreduce.job.queuename", "queue")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, col_fam)
    
    val fs: FileSystem = FileSystem.get(conf)
    
    //delete output path if already exists
    if(fs.exists(new Path(outputPath))) {
      fs.delete(new Path(outputPath),true)
    }
    
    // Load an RDD of (row key, row Result) tuples from the table
    val hbaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
                 classOf[org.apache.hadoop.hbase.client.Result])
    
    val resultRDD = hbaseRDD.map(tuple=>tuple._2)
    val infoRDD = resultRDD.map(PInfo.parsePInfo)
    val infoDF = infoRDD.toDF()
    
    println("###########################################################################")
    infoDF.printSchema()
    infoDF.show()
    
    //for multioutput extract
    infoDF.write.partitionBy("rowKey").save(outputPath)
    
  }
}