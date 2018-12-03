package com.concept.dataset;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DataframeToSet {

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
		
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		//Dataset<Row> fd=spark.createDataFrame(rddinput, schema);
		Dataset<Row> df=spark.read().schema(schema).csv("/home/purva/spark-data/MaleFemaleSQl/input.csv");
		Dataset<Person> peopleDS = df.as(personEncoder);
		peopleDS.show();

		
	}

}
