package com.spark.usecases.hbase

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.KeyOnlyFilter
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter
import org.apache.hadoop.hbase.client.ResultScanner
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Delete
import scala.collection.JavaConverters._
    

object HBaseDelete {
  
  final val tableName = "hbase_table"
  final val outputPath = "PATH_TO_FILE"
  final val cols = "id,name"
  final val col_fam = "info"
  final val col_Arr: Array[String] = cols.split(",")
  
  def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Spark Hbase"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    val conf = HBaseConfiguration.create()
    conf.set("mapreduce.job.queuename", "queue")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, col_fam)
    
    val hTable = new HTable(conf,tableName)
    
    val cal: Calendar = Calendar.getInstance()
        cal.set(Calendar.HOUR_OF_DAY, 0)
		    cal.set(Calendar.MINUTE, 0)
		    cal.set(Calendar.SECOND, 0)
		    cal.set(Calendar.MILLISECOND, 0)
    val today: Long  = cal.getTimeInMillis()  
    
    val scan: Scan = new Scan()
        scan.setTimeRange(0, today) 
    // Crucial optimization: Retrieve only row keys
    val filters: FilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,
                    new FirstKeyOnlyFilter(), new KeyOnlyFilter())
    
    scan.setFilter(filters)
    
    val resultScanner: ResultScanner = hTable.getScanner(scan)
    val deletes = List.newBuilder[Delete]
    var rr = Array[Result]()
    val rL = List.newBuilder[Result]
    
    resultScanner.asScala.foreach { r => 
       var delete: Delete = new Delete(r.getRow(),today)
       deletes+=delete
       rL+=r
    }
      println("memebrs of delete list ::::::::::::: " +deletes.result())
      println("resultscanner list size::::::::::::: "+rL.result())
      
      hTable.delete(deletes.result().asJava) 
    
  }
}