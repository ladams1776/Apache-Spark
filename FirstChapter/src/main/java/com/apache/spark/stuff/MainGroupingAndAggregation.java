package com.apache.spark.stuff;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class MainGroupingAndAggregation {

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

    Dataset<Row> students =
        sparkSession.read().option("header", true).csv("src/main/resources/exams/students.csv");

    final List<Row> inMemory = new ArrayList<>();

    inMemory.add(RowFactory.create("WARN", "16 December 2018"));
    inMemory.add(RowFactory.create("FATAL", "16 December 2018"));
    inMemory.add(RowFactory.create("INFO", "16 December 2018"));
    inMemory.add(RowFactory.create("FATAL", "16 December 2018"));

    final StructField[] fields = new StructField[]{
        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
    };

    StructType schema = new StructType(fields);

    Dataset<Row> dataset = sparkSession.createDataFrame(inMemory, schema);

    dataset.createOrReplaceTempView("logging_table");

    // because we are grouping on the level, we are now aggregating. But when when we
    // do this, we also need to call a scalar like function, i.e. average, count, etc.
    // so below won't work. But before that it will
//    Dataset<Row> sql = sparkSession.sql("select level, datetime from logging_table group by level");
    Dataset<Row> sql = sparkSession.sql("select level, collect_list(datetime) from logging_table group by level order by level");


    sql.show();
    sparkSession.close();
  }
}
