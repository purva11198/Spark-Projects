package com.concept.trial;

import static org.apache.spark.sql.functions.col;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataFrames {
	public static void main(String[] args) {
		
		SparkSession spark = SparkSession
				  .builder()
				  .appName("Java Spark SQL basic example")
				  .config("spark.some.config.option", "some-value")
				  .master("local")
				  .getOrCreate();
		
		Dataset<Row> df=spark.read().json("/home/purva/spark-data/TweetsProject/try.json");
		Dataset<Row> df1=spark.read().text("/home/purva/spark-data/WordCount/input.txt");
		//df.show();
		//df1.show();
		//df.printSchema();
		//df.select("name").show();
		df.select("name").show(5);
		//df.select(col("name"), col("roll").plus(1)).show();
	
		//df.filter(col("roll").gt(1)).show();
		//df.groupBy("roll").count().show();
		df.first();
		
		
		JavaRDD<Row> rd=df.toJavaRDD();
		rd.foreach(item -> {
			System.out.println(item);
			
		});
		
	}

}
