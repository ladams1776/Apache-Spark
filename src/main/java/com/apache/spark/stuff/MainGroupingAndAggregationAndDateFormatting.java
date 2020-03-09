package com.apache.spark.stuff;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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

public class MainGroupingAndAggregationAndDateFormatting {

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

    inMemory.add(RowFactory.create("WARN", "20/03/09 09:11:36"));
    inMemory.add(RowFactory.create("FATAL", "20/03/09 09:11:36"));
    inMemory.add(RowFactory.create("INFO", "20/03/09 09:11:36"));
    inMemory.add(RowFactory.create("FATAL", "20/03/09 09:11:36"));

    final StructField[] fields = new StructField[]{
        new StructField("level", DataTypes.StringType, false, Metadata.empty()),
        new StructField("datetime", DataTypes.StringType, false, Metadata.empty())
    };

    StructType schema = new StructType(fields);

    Dataset<Row> dataset = sparkSession.createDataFrame(inMemory, schema);

    dataset.createOrReplaceTempView("logging_table");

    //@TODO: This is not working atm. Got to shelf it for the moment.
    Dataset<Row> sql = sparkSession.sql("select level, date_format(datetime, 'y') from logging_table");


    sql.show();
    sparkSession.close();
  }
}
