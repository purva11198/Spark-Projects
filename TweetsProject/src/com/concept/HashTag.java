package com.concept;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class HashTag implements FlatMapFunction<String,String>{

	@Override
	public Iterator<String> call(String text) throws Exception {
		ArrayList<String> al=new ArrayList<>();
		String ar[]=text.split(" ");
		for(int j=0;j<ar.length;j++) {
			if(ar[j].contains("#")) {
				al.add(ar[j]);
			}
		}
		
		return al.iterator();
	}



}
