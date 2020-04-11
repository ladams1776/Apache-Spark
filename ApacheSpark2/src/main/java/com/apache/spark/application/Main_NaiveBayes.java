package com.apache.spark.application;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;

public class Main_NaiveBayes {
  SparkConnection sparkConnection = new SparkConnectionBuilder().build();
  SparkSession sparkSession = sparkConnection.getSparkSession();

  public static void main(String[] arg) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    System.out.println("NaiveBayes");
  }

}
