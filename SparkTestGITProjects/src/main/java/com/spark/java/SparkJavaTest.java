package com.spark.java;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class SparkJavaTest {
	public static void main(String[] args) {
		System.out.println("Scala-Java Maven Integration is successful!!");
		
		Calendar cal  = Calendar.getInstance();
		cal.set(Calendar.HOUR_OF_DAY, 3);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
	    Long today   = cal.getTimeInMillis();
	    System.out.println(today);
	    
	}
	
	 
}
