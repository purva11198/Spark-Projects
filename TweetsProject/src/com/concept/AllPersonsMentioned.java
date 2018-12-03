package com.concept;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AllPersonsMentioned {
	public static void main(String[] args) {
		
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("MyAllTweets");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String input="/home/purva/spark-data/TweetsProject/input.json";
		
		JavaRDD<String> fileData = javaSparkContext.textFile(input);
		JavaRDD<Person> result = fileData.mapPartitions(new ParseJson());
		
		
		JavaRDD<String> persons=result.flatMap(new FlatMapFunction<Person, String>() {

			@Override
			public Iterator<String> call(Person details) throws Exception {
				ArrayList<String> al=new ArrayList<>();
				String ar[]=details.text.split(" ");
				for(int j=0;j<ar.length;j++) {
					if(ar[j].contains("@")) {
						al.add(ar[j]);
					}
				}
				Collections.sort(al);
				return al.iterator();
			}
			
		});
		
		
		System.out.println("Mentioned persons are ");
		persons.foreach(item -> {
			
			System.out.println(item+" ");
			
		}	
		);
		
		JavaPairRDD<String, Integer> personMap=persons.mapToPair(new PairFunction<String, String, Integer>() {

			@Override
			public Tuple2<String, Integer> call(String test) throws Exception {
				// TODO Auto-generated method stub
				Tuple2<String, Integer> vals=new Tuple2<String, Integer>(test, 1);
				return vals;
			}
		});
		
		JavaPairRDD<String, Integer> totalpersons=personMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer tweet1, Integer tweet2) throws Exception {
				return tweet1+tweet2;
			}
		});

		totalpersons.foreach(item -> {
			
			System.out.println(item._1 +" : "+item._2);
			
		}	
		);
		System.out.println("Top 10 are ");
		System.out.println(totalpersons.top(10));
	}
}
