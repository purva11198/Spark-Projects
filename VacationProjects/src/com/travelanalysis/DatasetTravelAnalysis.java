package com.travelanalysis;

import java.util.Arrays;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class DatasetTravelAnalysis {

	public static void main(String[] args) {
		
		SparkSession spark=SparkSession.builder().master("local").appName("travel analysis").getOrCreate();

		StructType schema=DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("citypair", DataTypes.StringType, true),
				DataTypes.createStructField("from", DataTypes.StringType, true),
				DataTypes.createStructField("to", DataTypes.StringType, true),
				DataTypes.createStructField("producttype", DataTypes.IntegerType, true),
				DataTypes.createStructField("adult", DataTypes.IntegerType, true),
				DataTypes.createStructField("senior", DataTypes.IntegerType, true),
				DataTypes.createStructField("children", DataTypes.IntegerType, true),
				DataTypes.createStructField("youth", DataTypes.IntegerType, true),
				DataTypes.createStructField("infant", DataTypes.IntegerType, true),
				DataTypes.createStructField("dot-tot", DataTypes.StringType, true),
				DataTypes.createStructField("dor-tor", DataTypes.StringType, true),
				DataTypes.createStructField("airprice", DataTypes.FloatType, true),
				DataTypes.createStructField("carprice", DataTypes.FloatType, true),
				DataTypes.createStructField("hotelprice", DataTypes.FloatType, true),
				DataTypes.createStructField("janekiflight", DataTypes.StringType, true),
				DataTypes.createStructField("aanekflight", DataTypes.StringType, true),
				DataTypes.createStructField("carname", DataTypes.StringType, true),
				DataTypes.createStructField("hotel", DataTypes.StringType, true)
				));
		
		//can take input like this
		//Dataset<Row> inpu=spark.read().option("sep", "\t").schema(schema)
		//			.csv("/home/purva/spark-data/VacationProjects/travelanalysis/TravelData.txt");
		
		
		Dataset<Row> inp=spark.read().format("csv").option("delimiter", "\t").schema(schema)
				.load("/home/purva/spark-data/VacationProjects/travelanalysis/TravelData.txt");
		//inp.printSchema();
		
//top 20 destinations
		Dataset<Row> todataset=inp.select(inp.col("to"));
		Dataset<String> des=todataset.map((MapFunction<Row,String>)r->r.toString() , Encoders.STRING());
		Dataset<Row> desf = des.groupBy("value") //<k, iter(V)>
	    		.count()
	    		.toDF("desn","count");
		 desf = desf.sort(functions.desc("count"));
		//desf.show();
		 
//top 20 from
		Dataset<Row> fromdataset=inp.select(inp.col("to"));
		Dataset<String> from=fromdataset.map((MapFunction<Row,String>)r->r.toString() , Encoders.STRING());
		Dataset<Row> fromf = from.groupBy("value") //<k, iter(V)>
		   		.count()
		   		.toDF("desn","count");
		fromf = fromf.sort(functions.desc("count"));
		//fromf.show();
			 
//top 20 cities that generate high air revenue
		Dataset<Row> airdataset=inp.filter(inp.col("producttype").equalTo(1))
				.select(inp.col("to"),inp.col("airprice"))
				.sort(inp.col("airprice").desc());
		Dataset<String> air=airdataset.map((MapFunction<Row,String>)r->r.toString() , Encoders.STRING());
		//airdataset.show();

//best and cheapest hotel
		
		//converting a dataset<Row> to string means it converts as[] array of values 
		
	//hotel names can be same in 2 cities..so for uniqueness we are taking hotel name+city
		Dataset<Row> hotelin1=inp.select(functions.concat(inp.col("hotel"),inp.col("to")));
		Dataset<Row> hotelin2 = hotelin1.groupBy(hotelin1.col("concat(hotel, to)")) 
		   		.count()
		   		.toDF("hotel","count");
		Dataset<Row> hotelin3 = hotelin2.sort(functions.desc("count"));
		Dataset<Row> hotelin4=hotelin3.select(hotelin3.col("hotel"));
		Dataset<Row> hotelSorted=hotelin4.withColumn("hotelid", functions.row_number().over(Window.orderBy("hotel")));
		
	//cheapest hotel sorting
		Dataset<Row> cheap1=inp.select(functions.concat(inp.col("hotel"),inp.col("to")),inp.col("hotelprice"))
				.filter(s -> !s.anyNull()).toDF("hotell","hotelprice");
		Dataset<Row> cheap2=cheap1.sort(functions.asc("hotelprice"));
		Dataset<Row> cheap3=cheap2.select(cheap1.col("hotell"));
		Dataset<Row> cheapSorted=cheap3.withColumn("cheapid", functions.row_number().over(Window.orderBy("hotell")));
				
	//joing n sorting
		Dataset<Row> joinset=hotelSorted.join(cheapSorted,hotelSorted.col("hotel").equalTo(cheapSorted.col("hotell")));
		//joinset.printSchema();
		//another way to join
		//Dataset<Row> tr2=hotelSorted.as("hotelSorted").join(cheapSorted.as("cheapSorted"),cheapSorted.col("hotel").equalTo(hotelSorted.col("hotel")));
		Dataset<Row> bestHotel=joinset.select(joinset.col("hotel"),joinset.col("hotelid").plus(joinset.col("cheapid")));
		//bestHotel.show();
		
//best duration for each destintion
		//here we have taken date n time in string type .but we can directly have timestamp type..for that we have extra minute,seconds function
		Dataset<Row> timedataset=inp.select(inp.col("to"),inp.col("dot-tot"),inp.col("dor-tor"));
		Dataset<Row> t1=timedataset.withColumn("dot", timedataset.col("dot-tot").substr(0, 10))
			.withColumn("dor", timedataset.col("dor-tor").substr(0, 10));
		Dataset<Row> t2=t1.select(t1.col("to"),t1.col("dot").cast("date"),t1.col("dor").cast("date"));
		//how to select year,month,day from date type
		//t2.select(functions.year(t2.col("dot")),functions.month(t2.col("dot")),functions.dayofmonth(t2.col("dot"))).show();
		//finding duration of travel
		Dataset<Row> t3=t2.withColumn("duration_of_travel",functions.datediff(t2.col("dot"), t2.col("dor"))
				);
		Dataset<Row> t4=t3.sort(t3.col("duration_of_travel").desc());
		//Dataset<Row> t5=t4.groupBy(t4.col("to")).sum((Seq<String>) t4.col("duration_of_travel"));
		//t5.show();
		
	}
}
