package com.concept.dataset;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.Collections;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;


public class DatasetTrial {
	public static void main(String[] args) {
		
		SparkSession spark=SparkSession
				.builder()
				.master("local")
				.getOrCreate();
		
		
		// Create an instance of a Bean class
		Person person = new Person();
		person.setId(30);
		person.setGender("male");
		person.setAge(32);
		person.setT1(32);
		person.setT2(21);
		person.setT3(98);

		// Encoders are created for Java beans
		Encoder<Person> personEncoder = Encoders.bean(Person.class);
		Dataset<Person> javaBeanDS = spark.createDataset(
		  Collections.singletonList(person),
		  personEncoder
		);
		javaBeanDS.show();
	
	
		// Encoders for most common types are provided in class Encoders
		Encoder<Integer> integerEncoder = Encoders.INT();
		Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
		Dataset<Integer> transformedDS = primitiveDS.map(
		    (MapFunction<Integer, Integer>) value -> value + 1,
		    integerEncoder);
		transformedDS.collect(); // Returns [2, 3, 4]
	
		
		// DataFrames can be converted to a Dataset by providing a class. Mapping based on name
		StructType schema=DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("id", DataTypes.IntegerType, true),
				DataTypes.createStructField("gender", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("t1", DataTypes.IntegerType, true),
				DataTypes.createStructField("t2", DataTypes.IntegerType, true),
				DataTypes.createStructField("t3", DataTypes.IntegerType, true)
				));
		
		String path = "/home/purva/spark-data/MaleFemaleSQl/input.csv";
		Dataset<Person> peopleDS = spark.read().schema(schema).csv(path).as(personEncoder);
		peopleDS.show();
	
	
	
	
	}
}