package com.travelanalysis;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDTravelAnalysis {

	public static void main(String[] args) {

		SparkConf sparkConf=new SparkConf();
		sparkConf.setMaster("local");
		sparkConf.setAppName("travel analysis");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String inputpath="/home/purva/spark-data/VacationProjects/travelanalysis/TravelData.txt";
		
		JavaRDD<String> input=javaSparkContext.textFile(inputpath);
				
		JavaRDD<ArrayList<String>> fields=input.flatMap(s -> s.split("\t")).filter(s -> !s.isEmpty());

		fields.foreach(item -> {
			
			System.out.println(item);
			
		}	
		);
		
		System.out.println(fields);
		
	}

}
