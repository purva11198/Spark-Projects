package com.concept.dataset.prac;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class StructuredSpark1 {
	public static void main(String[] args) {
		//manipulation of data
			SparkSession spark = SparkSession.builder().master("local").appName("faltu").getOrCreate();
				String input1="/home/purva/spark-data/MaleFemaleSQl/input.csv";
				Dataset<Row > csvinput=spark.read().csv(input1);
				csvinput.printSchema();
				
				//manipulation of values of a column
				Dataset<Row> data1=csvinput.withColumn("age",org.apache.spark.sql.functions.when( csvinput.col("_c2").lt(25),"young")
						.when(csvinput.col("_c2").gt(25).and(csvinput.col("_c2").lt(50)),"old")
						.otherwise("senior")
						);
				//data1.select(data1.col("age")).show();
				
				//manipulation of columns
				data1.printSchema();
				String exp="_c3+_c4+_c5";
				Dataset<Row> tr=data1.select(data1.col("_c0"),org.apache.spark.sql.functions.expr(exp).as("sum"));
				tr.sort(org.apache.spark.sql.functions.desc("sum")).show();
				tr.sort(tr.col("sum").desc());
				
//				//manipulating n no of cols
//				String[] ar= {"_c2","_c3","_c4"};
//				ArrayList<String> exparr=new ArrayList<String>(Arrays.asList(ar));
//				String exp2=StringUtils.join(exparr,"+");
//				
//				data1.groupBy(data1.col("age")).agg(org.apache.spark.sql.functions.avg("_c3").as("avg c3"),
//						org.apache.spark.sql.functions.avg("_c4").as("avg c4"))
//				.show();
	}
}
