package com.apache.spark.infrastructure;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

public class SparkConnection {

  private final static String TEMP_DIR = "file:///c:/temp/spark-warehouse";
  private final static String APP_NAME = "Linear Regression Example";
  private final static String SPARK_MASTER = "local[2]";

  private JavaSparkContext spContext = null;
  private SparkSession sparkSession;

  private void getConnection() {
    if (this.spContext == null) {
      SparkConf sparkConf = new SparkConf()
          .setAppName(APP_NAME)
          .setMaster(SPARK_MASTER);

      System.setProperty("hadoop.home.dir", "C:\\spark\\winutils\\");

      this.spContext = new JavaSparkContext(sparkConf);

      this.sparkSession = SparkSession
          .builder()
          .appName(APP_NAME)
          .master(SPARK_MASTER)
          .config("spark.sql.warehouse.dir", TEMP_DIR)
          .getOrCreate();
    }
  }

  public JavaSparkContext getSpContext() {
    if (this.spContext == null) {
      this.getConnection();
    }

    return this.spContext;
  }

  public SparkSession getSparkSession() {
    if (this.sparkSession == null) {
      getConnection();
    }
    return this.sparkSession;
  }

  public static class SparkConnectionBuilder {

    public SparkConnection build() {
      return new SparkConnection();
    }
  }
}
