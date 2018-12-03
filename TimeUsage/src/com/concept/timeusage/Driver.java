package com.concept.timeusage;

import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Driver {

	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local").appName("time usage project").getOrCreate();

		String inputPath = "/home/purva/spark-data/TimeUsage/input.csv";

		// here inferschema as when we want to perform addtion it is difficult for spark
		// to type cast more than 100 records..for small records it can work
		Dataset<Row> input = spark.read().format("csv").option("header", "true").option("inferschema", "true")
				.load(inputPath);

		String ar[] = input.columns();

		String pri[] = { "t01", "t03", "t11", "t1801", "t1803" };
		ArrayList<String> primary = new ArrayList<>();

		String work[] = { "t05", "t1805" };
		ArrayList<String> wo = new ArrayList<>();

		String other[] = { "t02", "t04", "t06", "t07", "t08", "t09", "t10", "t12", "t13", "t14", "t15", "t16", "t18" };
		ArrayList<String> oth = new ArrayList<>();

		for (int i = 0; i < ar.length; i++) {
			for (int j = 0; j < pri.length; j++) {

				if (ar[i].startsWith(pri[j])) {
					primary.add(ar[i]);
				}
			}
		}

		for (int i = 0; i < ar.length; i++) {
			for (int j = 0; j < work.length; j++) {

				if (ar[i].startsWith(work[j])) {
					wo.add(ar[i]);
				}
			}
		}

		for (int i = 0; i < ar.length; i++) {
			for (int j = 0; j < other.length; j++) {

				if (ar[i].startsWith(other[j])) {
					oth.add(ar[i]);
				}
			}
		}

		String exp1 = StringUtils.join(primary, "+");
		String exp2 = StringUtils.join(wo, "+");
		String exp3 = StringUtils.join(oth, "+");

		// no need to select every record
		// Dataset<Row>
		// tr=input.select(input.col("tucaseid"),input.col("gemetsta"),input.col("gtmetsta"),input.col("peeduca")
		// ,input.col("pehspnon"),input.col("ptdtrace"),input.col("teage")
		// ,input.col("telfs"),input.col("temjot")
		// ,input.col("teschenr"),input.col("teschlvl"),input.col("tesex"),input.col("tespempnot")
		// ,input.col("trchildnum"),input.col("trdpftpt"),input.col("trernwa"),input.col("trholiday")
		// ,input.col("trspftpt"),input.col("trsppres"),input.col("tryhhchild"),input.col("tudiaryday")
		// ,input.col("tufnwgtp"),input.col("tehruslt"),input.col("tuyear")
		// ,org.apache.spark.sql.functions.expr(exp1).as("primary")
		// ,org.apache.spark.sql.functions.expr(exp2).as("work")
		// ,org.apache.spark.sql.functions.expr(exp3).as("other")
		// );

		// dividing age group young,active,elder
		Dataset<Row> tr = input.withColumn("age",
				org.apache.spark.sql.functions.when(input.col("teage").lt(25), "young")
						.when(input.col("teage").gt(25).and(input.col("teage").lt(50)), "active").otherwise("elder"));

		// giving male,female
		Dataset<Row> data1 = tr.withColumn("sex", org.apache.spark.sql.functions
				.when(tr.col("tesex").equalTo("1"), "male").when(tr.col("tesex").equalTo("2"), "female"));

		// taking sum and naming it primary
		Dataset<Row> tr1 = data1.select(data1.col("tucaseid"), data1.col("age"), data1.col("sex"),
				org.apache.spark.sql.functions.expr(exp1).as("primary"),
				org.apache.spark.sql.functions.expr(exp2).as("work"),
				org.apache.spark.sql.functions.expr(exp3).as("other"));

		// data2.show();

		tr1.groupBy(tr1.col("age"), tr1.col("sex"))
				.agg(org.apache.spark.sql.functions.avg("primary").as("avg primary"),
						org.apache.spark.sql.functions.avg("work").as("avg work"),
						org.apache.spark.sql.functions.avg("other").as("avg other"))
				.show();

	}

}
