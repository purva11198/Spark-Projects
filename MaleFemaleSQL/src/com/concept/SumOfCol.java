package com.concept;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SumOfCol {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		SparkSession spark=SparkSession
				.builder()
				.master("local")
				.getOrCreate();
		
		StructType schema=DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("id", DataTypes.IntegerType, true),
				DataTypes.createStructField("gender", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("t1", DataTypes.IntegerType, true),
				DataTypes.createStructField("t2", DataTypes.IntegerType, true),
				DataTypes.createStructField("t3", DataTypes.IntegerType, true)
				));
		
		Dataset<Row> df=spark.read().schema(schema).csv("/home/purva/spark-data/MaleFemaleSQl/input.csv");
		//df.printSchema();
		//df.show();
		
		//person who spends t2<5
		//df.filter(col("t2").lt(5)).show();
		
		//male who spends t1>5
		//df.filter(col("gender").equalTo("male")).filter(col("t1").gt(5)).show();
		
		//female who spends t1>5
		//df.filter(col("gender").equalTo("female")).filter(col("t3").lt(5)).show();
		
		//total time spend by male for each activity t1,t2,t3
		//df.filter(col("gender").equalTo("male")).groupBy("gender").sum("t1","t2","t3").show();

		//total time spend by female for each activity t1,t2,t3
		//df.filter(col("gender").equalTo("female")).groupBy("gender").sum("t1","t2","t3").show();
		
		//total time spend by male and female for each activity
		//df.groupBy("gender").sum("t1","t2","t3").show();
		
		//t1,t2,t3 for younger ie below 25
		//df.filter(col("age").lt(25)).show();
		
		//sum of every column
		//df.groupBy().sum().show();
		
		//total time
		

	}

}
