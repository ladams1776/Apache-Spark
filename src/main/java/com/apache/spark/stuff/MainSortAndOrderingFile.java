package com.apache.spark.stuff;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Scope.col;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class MainSortAndOrderingFile {

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

    Dataset<Row> logs =
        sparkSession.read()
            .option("header", true)
            .csv("src/main/resources/biglog.txt");

    logs.createOrReplaceTempView("logging_table");

    // - SQL Spark API

//    System.out.println("SQL Spark");
//    // traditional way of doing an order by.
//    final Dataset<Row> sql = sparkSession.sql(
//        "select level, "
//            + "cast(date_format(datetime, 'M') as int) as month, "
//            + "count(1) as total "
//            + " from logging_table "
//            + "  group by level, month "
//            + "  order by month, level");
//    sql.show(100);
//
//    System.out.println("Inline ordering the Dataset");
//    // could also do the order this way too.
//    final Dataset<Row> sql1 = sparkSession.sql(
//        "select level, "
//            + "count(1) as total "
//            + " from logging_table "
//            + "  group by level, datetime "
//            + "  order by cast(date_format(datetime, 'M') as int)");
//    sql1.show(100);
//
//    // DataFrames API
//
//    System.out.println("DataFrames API");
//
//    // This is a mid way. Using DataFrames with Spark SQL.
//    logs.selectExpr("level", "date_format(datetime, 'M') as months");
//    logs.show(100);
//
//    System.out.println("Group by with DataFrames API");
//    // This is a full way into DataFrames and no Spark SQL.
//    Dataset<Row> df = logs.select(col("level"),
//        date_format(col("datetime"), "MMM").as("months")
//    );
//    df = df.groupBy(col("level"), col("months")).count();
//    df.show(100);

    System.out.println("A more complex group by with DataFrames API");
    Dataset<Row> df = logs.select(col("level"),
        date_format(col("datetime"), "MMM").as("months"),
        date_format(col("datetime"), "M").as("monthnums").cast(DataTypes.IntegerType)
    );
    df = df.groupBy(col("level"), col("months"), col("monthnums")).count();
    df = df.orderBy("monthnums");
    df.show(100);

    sparkSession.close();
  }
}
