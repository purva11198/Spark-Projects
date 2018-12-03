package com.concept;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
	public Iterator<Person> call(Iterator<String> lines) throws Exception {
		ArrayList<Person> people = new ArrayList<Person>();
		ObjectMapper mapper = new ObjectMapper();
		while (lines.hasNext()) {
			String line = lines.next();
			try {
				people.add(mapper.readValue(line, Person.class));
			} catch (Exception e) {
				// skip records on failure
			}
		}
		return people.iterator();
	}
}