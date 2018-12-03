package www.concept;

import org.apache.spark.api.java.function.Function2;

public class Count implements Function2<Integer, Integer, Integer> {

	@Override
	public Integer call(Integer val1, Integer val2) throws Exception {
	
		return val1+val2;
	}

}
