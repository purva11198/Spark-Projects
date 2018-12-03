package com.concept.trial;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrameProgram {
public static void main(String[] args) throws AnalysisException {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .master("local")
				  .getOrCreate();
		Dataset<Row> df=spark.read().json("/home/purva/spark-data/TweetsProject/try.json");
//		df.createOrReplaceTempView("people");
//		Dataset<Row> dfsql=spark.sql("select * from people");
//		dfsql.show();
		
		
		df.createGlobalTempView("people");

		// Global temporary view is tied to a system preserved database `global_temp`
		//spark.sql("SELECT * FROM global_temp.people").show();
	
		Dataset<Row> dfsql=spark.sql("select * from global_temp.people");
		dfsql.show();
		
		//Global temporary view is cross-session
		//spark.newSession().sql("SELECT * FROM global_temp.people").show();
		
}
}
