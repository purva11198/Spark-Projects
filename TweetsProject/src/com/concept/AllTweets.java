package com.concept;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class AllTweets {

	static HashMap<String, Integer> hm1=new HashMap<>();
	static HashMap<String, String> hm2=new HashMap<>();
	static HashMap<String, Integer> hm3=new HashMap<>();

	
	public static void main(String[] args) {
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("MyAllTweets");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String input="/home/purva/spark-data/TweetsProject/input.json";
		//String output="/home/purva/spark-data/TweetsProject/output.txt";
		
		JavaRDD<String> fileData = javaSparkContext.textFile(input);
	
		JavaRDD<Person> result = fileData.mapPartitions(new ParseJson());
		
		//user deatils madhun apan na text ghetla
		JavaRDD<String> text=result.map(s -> s.getText());
	
		//split karun aaplyala array milala...flat map madhe iterator return hota so apan tya array la list madhe convert kela
		JavaRDD<String> textwords=text.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		
		//tya madhun fakt hash wale wors ghetle ..filter madhe boolean return karne wala function hota
		JavaRDD<String> tags=textwords.filter(s -> s.contains("#"));
		
		//tags,1
		JavaPairRDD<String, Integer> tagsAndOnes=tags.mapToPair(s -> new Tuple2<>(s,1)); 
		
		//sum kara 
		JavaPairRDD<String, Integer> tagsAndTotal=tagsAndOnes.reduceByKey((x,y) -> (x+y));
		
		//JavaPairRDD<String, Iterable<Integer>> test=tagsAndOnes.groupByKey();
		
		//action lo 
		
}
}
