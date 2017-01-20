package com.spark.usecases.hdfs

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileStatus
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.hadoop.fs.FileUtil
import java.io.File


object HDFSPurge {
  
  var sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
  
  def main(args: Array[String]) {
    
    val sc = new SparkContext(new SparkConf().setAppName("Spark HDFS"))
    val inputPath = args(0)
    val outputPath = args(1)
    val purgeDays = args(2).toInt
    
    
    val date: Date = new Date()
    val formated_date: String = sdf.format(date)
    var purgeDate: Date = null
    
    purgeDate = sdf.parse(formated_date)
    
    val cal: Calendar = Calendar.getInstance()
        cal.setTime(purgeDate)
        cal.add(Calendar.DAY_OF_YEAR, -purgeDays)
    
    purgeDate = cal.getTime()
    
    val purgePeriod: String = sdf.format(purgeDate)
    
    println("Purge period entered is :"
				+ purgeDays + " days. Files for " + purgePeriod
				+ " will be deleted")
    
		//doArchieve(inputPath, outputPath, formated_date, purgePeriod, purgeDate)
		doArchieveFileUtil(outputPath)	//to copy local files to HDFS		
	
  }
  
    def doArchieve(inputPath: String, outputPath: String, formated_date: String, purgePeriod: String, purgeDate: Date): Unit = {
      println("Entering doArchive!!!")
      
      val conf: Configuration = new Configuration()
      val fs: FileSystem = FileSystem.get(conf)
      
      var deletePath: Path = new Path(inputPath + "/" + purgePeriod)
      val outputCopyPath: Path = new Path(outputPath)
      
      var purgeDate1: Date = purgeDate 
      
      while(fs.exists(deletePath)) {
         
         fs.rename(deletePath, outputCopyPath) //take archieve 
         val isDeleted: Boolean = fs.delete(deletePath, true) //delete the previoues path
         
         val cal: Calendar = Calendar.getInstance()
             cal.setTime(purgeDate1)
             cal.add(Calendar.DAY_OF_YEAR, -1)
         var newPurgeDate = sdf.format(cal.getTime())
         println("New Purge Date::: " + newPurgeDate)
         deletePath = new Path(inputPath + "/" + newPurgeDate)
         println("New Purge Path::: " + deletePath.toString())
         purgeDate1 = cal.getTime()
         
      }
     
		}
    
    def doArchieveFileUtil(outputPath: String): Unit = {
      val conf: Configuration = new Configuration()
      val fs: FileSystem = FileSystem.get(conf)
      val outputCopyPath: Path = new Path(outputPath)
      val file: File = new File("PATH_TO_FILE")
      
      FileUtil.copy(file, fs, outputCopyPath, false, conf)//Copy local files to a FileSystem.
      
    }
}