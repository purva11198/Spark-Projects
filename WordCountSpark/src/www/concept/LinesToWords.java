package www.concept;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.spark.api.java.function.FlatMapFunction;

public class LinesToWords implements FlatMapFunction<String,String> {

	@Override
	public Iterator<String> call(String line) throws Exception {
		
		
		ArrayList<String> wordList=new ArrayList<>();
		String[] words=line.split(" ");
		for(String word:words) {
			wordList.add(word);
			}
		
		return wordList.iterator();
	}

}
