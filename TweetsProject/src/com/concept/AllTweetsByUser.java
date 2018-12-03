package com.concept;

import java.util.HashMap;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class AllTweetsByUser {

	public static void main(String[] args) {
		
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("MyAllTweets");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String input="/home/purva/spark-data/TweetsProject/input.json";
		
		JavaRDD<String> fileData = javaSparkContext.textFile(input);
		JavaRDD<Person> result = fileData.mapPartitions(new ParseJson());
//		
//		JavaPairRDD<String, String> userTweets=result.mapToPair(new PairFunction<Person, String, String>() {
//
//			@Override
//			public Tuple2<String, String> call(Person details) throws Exception {
//				Tuple2<String, String> val=new Tuple2<String, String>(details.getUser(),details.getText() );
//				return val;
//			}
//
//		});
//		
		
		JavaPairRDD<String, String> userTweets=result.mapToPair(details ->  new Tuple2<String, String>(details.getUser(),details.getText()));
		
//		JavaPairRDD<String, String> totalTweets=userTweets.reduceByKey(new Function2<String, String, String>() {
//			public String call(String tweet1, String tweet2) throws Exception {
//				return tweet1+"%%%%%"+tweet2;
//			}
//		});
		JavaPairRDD<String, String> totalTweets1=userTweets.reduceByKey((x,y)->(x+"&&&&"+y));
		
		//distributed printing slow
//		totalTweets.foreach(item -> {
//			
//			System.out.println(item._1 +"===");
//			System.out.println(item._2);
//			System.out.println();
//			
//		}	
//		);

		HashMap<String, String > hm=(HashMap<String, String>) totalTweets1.collectAsMap();
		Set< String> ids=hm.keySet();
		
	}

}
