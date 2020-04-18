package com.apache.spark.application;

import com.apache.spark.domain.shared.CleanseData;
import com.apache.spark.domain.shared.LabelMapper;
import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.kmeans.CarCleaner;
import com.apache.spark.infrastructure.kmeans.CarLabelAssembler;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Main_KMeansClustering {
    private final static SparkConnection sparkConnection = new SparkConnection.SparkConnectionBuilder().build();
    private final static SparkSession sparkSession = sparkConnection.getSparkSession();
    private final static JavaSparkContext sparkContext = sparkConnection.getSpContext();
    private final static LabelMapper<Dataset<Row>, Dataset<Row>> labelAssembler = new CarLabelAssembler(sparkContext, sparkSession);

    private final static CleanseData<Dataset<Row>> carCleaner = new CarCleaner(sparkSession);

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);

        // ******************** Setup ************************************* //
        final Dataset<Row> autoDF = sparkSession
                .read()
                .option("header", "true")
                .csv("ApacheSpark2/src/main/resources/auto-data.csv");
        autoDF.show(10);
        autoDF.printSchema();

        // ******************** Cleanse Data ************************************* //
        final Dataset<Row> cleansedCarData = carCleaner.apply(autoDF);
        final Dataset<Row> cleansedLabelPoint = labelAssembler.map(cleansedCarData);

        // ******************** Perform Machine Learning ************************************* //

        KMeans kmeans = new KMeans()
                .setK(4)
                .setSeed(1L);

        KMeansModel model = kmeans.fit(cleansedLabelPoint);
        Dataset<Row> predictions = model.transform(cleansedLabelPoint);

        System.out.println("Groupings : ");
        predictions.show(5);

        System.out.println("Groupings Summary : ");
        predictions.groupBy(col("prediction")).count().show();
    }
}
