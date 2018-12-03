package com.concept.dataset.prac;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

import com.concept.dataset.Person;

public class StructuredSpark {
	public static void main(String[] args) {
		String input="/home/purva/spark-data/TweetsProject/input.json";
		SparkSession spark = SparkSession.builder().master("local").appName("faltu").getOrCreate();
				
		// rdd
		Dataset<Row> frameinput = spark.read().json(input);
		//frameinput.printSchema();
		//System.out.println(frameinput.first().toString());
		
		//dtaframe to javardd
		JavaRDD<Row> test=frameinput.toJavaRDD();
		JavaRDD<String> test2 = test.map(s -> s.toString());
		
		
		//manipulations
		//he bagha nai yet a 
		//Dataset<Row> framemap = frameinput.map((MapFunction<Row,Row>) (Row row)-> row,Encoders.bean(Row.class));
		//framemap.show();
		
		//frameinput.select(frameinput.col("id").divide(87).equalTo(87)).show();
		//System.out.println(frameinput.getClass());		
	
	
		Dataset<String> ds=frameinput.map((MapFunction<Row,String>)r->r.toString() , Encoders.STRING());
		//ds.show();
		//System.out.println(ds.getClass());		

		//conversion of dataset to primitive datatypes
		////Dataset<String > dsss=spark.read().json(input).as(Encoders.STRING());
		//conversion of dataset to non-primitive datatypes
		////Dataset<Person> dsssssss=spark.read().json(input).as(Encoders.bean(Person.class));
		
		
		//converting any type of dataset to dataset<row> ie dataframe
		Dataset<Row> dff = ds.toDF();
		
		//conversio n of dataframe to rdd n again to frame
		JavaRDD<Row> rd=frameinput.toJavaRDD();
		//Dataset<Row> frameinput1=spark.createDataFrame(rd, schema); 
	
		//casting og columns
		Dataset<Row> castData=frameinput.withColumn("id",frameinput.col("id").cast(DataTypes.IntegerType));
		castData.printSchema();
		System.out.println("hello"+castData.select(castData.col("id")).getClass());
	
		
	}
}