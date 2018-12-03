package www.datasetwordcount;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WordCountDriver {

	public static void main(String[] args) {

		SparkSession spark=SparkSession.builder().master("local").appName("better word count").getOrCreate();
		
		//here we take input in dataset..but if we take in dataframe ie dataset row,while splitting it gives error..cant convert row to string
		Dataset<String> input = spark.read().text("/home/purva/spark-data/WordCount/input.txt").as(Encoders.STRING());
		 Dataset<String> words = input.flatMap(s -> {
				return  Arrays.asList(s.toLowerCase().split(" ")).iterator(); 
			}, Encoders.STRING())
			.filter(s -> !s.isEmpty())
			.coalesce(1); //one partition (parallelism level)
		 
		 //words.printSchema();
		 
		 Dataset<Row> t = words.groupBy("value") //<k, iter(V)>
		    		.count()
		    		.toDF("word","count");
		 
		    t = t.sort(functions.desc("count"));
		    
		    t.show();
	}

}
