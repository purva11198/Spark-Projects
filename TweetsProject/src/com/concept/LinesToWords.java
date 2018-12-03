package com.concept;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;

public class LinesToWords implements Function<Person,String> {

	public String call(Person line) throws Exception {
		// TODO Auto-generated method stub
		
				
		return line.getText();

	}

}
