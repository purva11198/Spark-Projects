import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonRead {

	public static void main(String[] args) {
		
		String input="/home/purva/spark-data/TweetsProject/input.json";

		
		SparkSession spark=SparkSession.builder().appName("read").master("local").getOrCreate();
		JavaRDD<Row> items=spark.read().json(input).toJavaRDD();
		items.foreach(item -> {
			System.out.println(item);
		}			
		);
	}

}
