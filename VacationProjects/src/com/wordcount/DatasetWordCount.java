package com.wordcount;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class DatasetWordCount {

	public static void main(String[] args) {

		SparkSession spark=SparkSession.builder().master("local").appName("better word count").getOrCreate();
		
		Dataset<String> input = spark.read().text("/home/purva/spark-data/VacationProjects/wordcount/dataset/MarkTwain.txt").as(Encoders.STRING());
		
		 Dataset<String> testwords = input.flatMap(s -> {
				return  Arrays.asList(s.split(" ")).iterator(); 
			}, Encoders.STRING())
			.filter(s -> !s.isEmpty())
			.coalesce(1); //one partition (parallelism level)
		
		 Dataset<String> words = testwords.map(s -> s.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ")
				 .toLowerCase(),Encoders.STRING());
		 
		 //words.printSchema();
		 
		 Dataset<Row> t = words.groupBy("value") //<k, iter(V)>
		    		.count()
		    		.toDF("word","count");
		 
		 //sorting descendingly
		 t = t.sort(functions.desc("count"));
		    
		 t.show();
		
		
	}

}
