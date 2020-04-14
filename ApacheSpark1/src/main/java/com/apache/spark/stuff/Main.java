package com.apache.spark.stuff;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

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
    // Display all the rows.
    //    students.show();
    // Display the count
    //    System.out.println(students.count());

    // Only display individual columns on the first rows
    //    System.out.println(students.first().get(SUBJECT_KEY).toString());
    //    System.out.println(students.first().getAs("subject").toString());
    //    System.out.println(students.first().getAs("year").toString());

    // Only display Modern Art
    //    students.filter("subject = 'Modern Art' ").show();

    // lambda
    students.filter(row -> row.getAs("subject").equals("Modern Art")).show();

    // 2 filters, one statement.
    students.filter("subject = 'Modern Art' AND year >= 2007 ").show();

    // columns style, 2 filters, easier to chain in this style than the other 2 styles.
    final Column subject = students.col("subject");
    final Column year = students.col("year");
    students.filter(subject.equalTo("Modern Art").and(year.equalTo("2007"))).show();

    // spark functions, 2 where predicates
    students
        .filter(col("subject").equalTo("Modern Art")
            .and(col("year").equalTo("2007")))
        .show();

    // take the raw dataset from students and create a table in memory, with any name that we like
    students.createOrReplaceTempView("students_view");
    sparkSession
        .sql("select score, year from students_view where subject='French' order by score desc")
        .show();

    sparkSession.close();
  }
}
