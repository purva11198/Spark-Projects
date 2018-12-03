package com.concept;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import io.fabric8.kubernetes.api.model.StatusFluent.DetailsNested;
import scala.Tuple2;

public class CountOfTweetsByUser {
public static void main(String[] args) {
		
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("MyAllTweets");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String input="/home/purva/spark-data/TweetsProject/input.json";
		
		JavaRDD<String> fileData = javaSparkContext.textFile(input);
		JavaRDD<Person> result = fileData.mapPartitions(new ParseJson());
		
//		JavaPairRDD<String, Integer> userTweets=result.mapToPair(new PairFunction<Person, String, Integer>() {
//
//			@Override
//			public Tuple2<String, Integer> call(Person details) throws Exception {
//				Tuple2<String, Integer> val=new Tuple2<String, Integer>(details.user,1);
//				return val;
//			}
//
//		});
		
		JavaPairRDD<String, Integer> userTweets=result.mapToPair(details -> new Tuple2<>(details.getUser(),1));
		
		JavaPairRDD<String, Integer> totalTweets=userTweets.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer tweet1, Integer tweet2) throws Exception {
				return tweet1+tweet2;
			}
		});

		totalTweets.foreach(item -> {
			
			System.out.println(item._1 +"===");
			System.out.println(item._2);
			System.out.println();
			
		}	
		);

	}
}
