package com.invertedindex;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;
import scala.reflect.api.TypeTags.TypeTag;
import shapeless.ops.tuple;


public class InvertedIndexDriver {

	public static void main(String[] args) {

		//spark.driver.allowMultipleContexts = true;
		SparkSession session=SparkSession.builder().master("local").appName("inverted index").getOrCreate();
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("MyInvertedIndex");
		sparkConf.setMaster("local");
		SparkContext sparkContext=session.sparkContext();
		
		String input="/home/purva/spark-data/VacationProjects/inverted-index/dataset/shakespeare/";
		
		RDD<Tuple2<String, String>> fileData=sparkContext.wholeTextFiles(input, 1);
		
		 JavaRDD<Tuple2<String, String>> x=fileData.toJavaRDD();
		
		 //System.out.println(x.first());
		
		 Dataset<Row> dataDF = session.createDataFrame(x, Encoders.tuple(Encoders.STRING(), Encoders.STRING()));
		 Dataset<Person> personDS = personDF.as(Encoders.bean(Person.class));
		 
	}

}
