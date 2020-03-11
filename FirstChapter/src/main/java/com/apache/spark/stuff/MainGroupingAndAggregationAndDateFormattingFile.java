package com.apache.spark.stuff;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MainGroupingAndAggregationAndDateFormattingFile {

  private static final int SUBJECT_KEY = 2;

  @SuppressWarnings("resource")
  public static void main(String[] args) {
    System.setProperty("hadoop.home.dir", "d:/hadoop");
    Logger.getLogger("org.apache").setLevel(Level.WARN);

    SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
    JavaSparkContext sc = new JavaSparkContext(conf);

    final SparkSession sparkSession =
        SparkSession.builder()
            .appName("testingSql") // name we see this in a profiler
            .master("local[*]") // how many cores
            .config("spark.sql.warehouse.dir", "file:///d:/tmp/") // required
            .getOrCreate();

    Dataset<Row> errors =
        sparkSession.read()
            .option("header", true)
            .csv("src/main/resources/biglog.txt");

    errors.createOrReplaceTempView("logging_table");
    final Dataset<Row> sql = sparkSession.sql(
        "select level, date_format(datetime, 'MMM') as month, count(1) as total from logging_table group by level, month");
    sql.show(100);

    sql.createOrReplaceTempView("results_table");
    Dataset<Row> totals = sparkSession.sql("select sum(total) from results_table");
    totals.show();

    sparkSession.close();
  }
}
