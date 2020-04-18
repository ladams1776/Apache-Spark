package com.apache.spark.infrastructure.kmeans;

import com.apache.spark.domain.shared.LabelMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.DoubleAccumulator;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.stddev;

public class CarLabelAssembler implements LabelMapper<Dataset<Row>,  Dataset<Row>> {

    private final JavaSparkContext sparkContext;
    private final SparkSession sparkSession;


    public CarLabelAssembler(JavaSparkContext sparkContext, SparkSession sparkSession) {
        this.sparkContext = sparkContext;
        this.sparkSession = sparkSession;
    }

    @Override
    public Dataset<Row> map(Dataset<Row> cleansedCarData) {
        final Row autoMeanRow = cleansedCarData.agg(avg(cleansedCarData.col("DOORS")),
                avg(cleansedCarData.col("BODY")),
                avg(cleansedCarData.col("HP")),
                avg(cleansedCarData.col("RPM")),
                avg(cleansedCarData.col("MPG")))
                .toJavaRDD()
                .takeOrdered(1)
                .get(0);


        final Row autoStdDev = cleansedCarData.agg(stddev(cleansedCarData.col("DOORS")),
                stddev(cleansedCarData.col("BODY")),
                stddev(cleansedCarData.col("HP")),
                stddev(cleansedCarData.col("RPM")),
                stddev(cleansedCarData.col("MPG")))
                .toJavaRDD()
                .takeOrdered(1)
                .get(0);

        System.out.printf("Automobile Mean: %s", autoMeanRow);
        System.out.printf("Automobile standard deviation: ", autoStdDev);

        final Broadcast<Row> bcAutoMean = sparkContext.broadcast(autoMeanRow);
        final Broadcast<Row> bcAuytoStdDev = sparkContext.broadcast(autoStdDev);
        final DoubleAccumulator rowId = sparkContext.sc().doubleAccumulator();

        // (mean for that row) - (value) / (std for that row)
        final JavaRDD<LabeledPoint> carDataLabeled = cleansedCarData.toJavaRDD().repartition(2).map(row -> {
            final double doors = (bcAutoMean.value().getDouble(0) - row.getDouble(0)) / bcAuytoStdDev.getValue().getDouble(0);
            final double body = (bcAutoMean.getValue().getDouble(1) - row.getDouble(1)) / bcAuytoStdDev.getValue().getDouble(1);
            final double hp = (bcAutoMean.getValue().getDouble(2) - row.getDouble(2)) / bcAuytoStdDev.getValue().getDouble(2);
            final double rpm = (bcAutoMean.getValue().getDouble(3) - row.getDouble(3)) / bcAuytoStdDev.getValue().getDouble(3);
            final double mpg = (bcAutoMean.getValue().getDouble(4) - row.getDouble(4)) / bcAuytoStdDev.getValue().getDouble(4);

            final Double id = rowId.value();
            rowId.setValue(id + 1);

            return new LabeledPoint(id,
                    Vectors.dense(doors, body, hp, rpm, mpg));
        });

        final Dataset<Row> autoVector = sparkSession.createDataFrame(carDataLabeled, LabeledPoint.class);
        System.out.printf("Centered and scaled vector: %s", autoVector);
        autoVector.show();
        return autoVector;
    }
}
