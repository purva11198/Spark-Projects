package www.concept;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		SparkConf sparkConf=new SparkConf();
		sparkConf.setAppName("MyWordCount");
		sparkConf.setMaster("local");
		JavaSparkContext javaSparkContext=new JavaSparkContext(sparkConf);
		
		String input="/home/purva/spark-data/WordCount/input.txt";
		String output="/home/purva/spark-data/WordCount/output.txt";
		
		JavaRDD<String> fileData=javaSparkContext.textFile(input);
		JavaRDD<String> words=fileData.flatMap(new LinesToWords());
		
		JavaPairRDD<String, Integer> wordsAndOnes=words.mapToPair(new PairFunction<String, String, Integer>() {
			@Override
			public Tuple2<String,Integer> call(String word ) throws Exception{ 
				Tuple2<String,Integer> temp=new Tuple2<String,Integer>(word,1);
				return temp;
			}
		});
//		JavaPairRDD<String, Integer> wordCount=wordsAndOnes.reduceByKey(new Function2<Integer,Integer,Integer>(){
//			@Override
//				public Integer call(Integer val1, Integer val2) throws Exception {
//					
//					return val1+val2;
//				}
//				});
		JavaPairRDD<String, Integer> wordCount=wordsAndOnes.reduceByKey((val1, val2) -> val1+val2);
	
		wordCount.saveAsTextFile(output);
	}

}
