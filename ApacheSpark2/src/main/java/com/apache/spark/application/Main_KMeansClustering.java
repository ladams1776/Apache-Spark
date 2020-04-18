package com.apache.spark.application;

import com.apache.spark.domain.shared.CleanseData;
import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.kmeans.CarCleaner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main_KMeansClustering {
    private final static SparkConnection sparkConnection = new SparkConnection.SparkConnectionBuilder().build();
    private final static SparkSession sparkSession = sparkConnection.getSparkSession();
    private final static CleanseData<Dataset<Row>> carCleaner = new CarCleaner(sparkSession);

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // ******************** Setup ************************************* //
        final Dataset<Row> autoDF = sparkSession
                .read()
                .option("header", "true")
                .csv("src/main/resources/auto-data.csv");
        autoDF.show(10);
        autoDF.printSchema();

        // ******************** Cleanse Data ************************************* //
        final Dataset<Row> cleansedCarData = carCleaner.apply(autoDF);



    }
}
