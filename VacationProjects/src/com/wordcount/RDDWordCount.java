package com.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class RDDWordCount {

	public static void main(String[] args) {
		
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("Better Word Count");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String inputpath="/home/purva/spark-data/VacationProjects/wordcount/dataset/MarkTwain.txt";
		
		JavaRDD<String>  input=javaSparkContext.textFile(inputpath);
		
		JavaRDD<String> testwords=input.flatMap(s -> Arrays.asList(s.split(" ")).iterator()).filter(s -> !s.isEmpty());
		
		JavaRDD<String> words = testwords.map(s -> s.replaceAll("[^\\w\\s]|('s|ly|ed|ing|ness) ", " ").toLowerCase());
		
		JavaPairRDD<String, Integer> wordAndOnes=words.mapToPair(s -> new Tuple2(s,1));
		
		JavaPairRDD<String, Integer> wordAndCount=wordAndOnes.reduceByKey((x,y)->(x+y));
		
		JavaPairRDD<Integer, String> swappedRDD=wordAndCount.mapToPair(s -> s.swap());
		
		JavaPairRDD<Integer, String> sortedSwappedRDD=swappedRDD.sortByKey();
		
		JavaPairRDD<String, Integer> sortedRDD=sortedSwappedRDD.mapToPair(s -> s.swap());
		
		sortedRDD.foreach(item -> {
			
			System.out.println(item._1+" "+item._2);
			
		}	
		);
		
	}

}
