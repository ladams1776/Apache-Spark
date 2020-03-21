package com.apache.spark.application;

import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Logger.getLogger;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

  private final static SparkConnection sparkConnection = new SparkConnectionBuilder().build();

  public static void main(String... args) {

    getLogger("org").setLevel(ERROR);
    getLogger("akka").setLevel(ERROR);

    System.out.println("Working");
    final JavaSparkContext spContext = sparkConnection.getSpContext();
    final SparkSession sparkSession = sparkConnection.getSparkSession();

    // Load the data

    final Dataset<Row> autoDF = sparkSession.read()
        .option("header", "true")
        .csv("src/main/resources/auto-miles-per-gallon.csv");

    autoDF.show(5);
    autoDF.printSchema();


  }
}
