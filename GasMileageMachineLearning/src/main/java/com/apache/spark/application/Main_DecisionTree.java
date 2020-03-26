package com.apache.spark.application;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class Main_DecisionTree {

  private static final SparkConnection sparkConnection = new SparkConnectionBuilder().build();

  public static void main(String[] args) {
    System.out.println("Working");

    final SparkSession sparkSession = sparkConnection.getSparkSession();
    final JavaSparkContext sparkContext = sparkConnection.getSpContext();
    


  }

}
