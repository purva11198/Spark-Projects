package com.aadharanalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class RDDAadharAnalysis {

	public static void main(String[] args) {

		SparkConf conf=new SparkConf();
		conf.setAppName("aadhar analysis");
		conf.setMaster("local");
		JavaSparkContext spark=new JavaSparkContext(conf);
		
		String input="/home/purva/spark-data/VacationProjects/aadhaar-dataset-analysis/data/input.csv";
		
		
		
	}

}
