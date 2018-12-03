package com.secondarysort;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DatasetSecondarySort {

	public static void main(String[] args) {
		SparkSession spark=SparkSession.builder().master("local").appName("travel analysis").getOrCreate();

		StructType schema=DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("lastname", DataTypes.StringType, true),
				DataTypes.createStructField("firstname", DataTypes.StringType, true)
				));
		
		String output="/home/purva/spark-data/VacationProjects/secondarysort/output/";
		
		Dataset<Row> inp=spark.read().format("csv").schema(schema)
				.load("/home/purva/spark-data/VacationProjects/secondarysort/dataset/Names.csv");
		//inp.printSchema();
		inp.repartition(inp.col("lastname")).sortWithinPartitions(inp.col("firstname")).write().csv(output);
	//coalesce to combine all the partition outputs in single file
		//inp.repartition(inp.col("lastname")).sortWithinPartitions(inp.col("firstname")).coalesce(1).write().csv(output);
	}

}
