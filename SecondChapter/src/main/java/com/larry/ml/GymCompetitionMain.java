package com.larry.ml;

import java.util.logging.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GymCompetitionMain {

  public static void main(String[] args) {
   System.setProperty("hadoop.home.dir", "c:/hadoop");
    Logger.getLogger("org.apache");

    final SparkSession spark = SparkSession.builder()
        .appName("Gym Competition")
        .master("local[*]").getOrCreate();

    Dataset<Row> csv = spark.read().option("header", true)
        .csv("src/main/resources/GymCompetition.csv");

    csv.show();


    spark.close();
  }
}
