package com.apache.spark.application;

import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Logger.getLogger;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Main_RandomForest {

  private final static SparkConnection sparkConnection = new SparkConnectionBuilder().build();

  public static void main(String[] args) {
    getLogger("org").setLevel(ERROR);
    getLogger("akka").setLevel(ERROR);

    // ******************** Setup ************************************* //
    final Dataset<Row> banks = sparkConnection.getSparkSession()
        .read()
        .option("header", "true")
        .option("sep", ";")
        .csv("src/main/resources/bank.csv");

    banks.show(5);
    banks.printSchema();

  }

}
