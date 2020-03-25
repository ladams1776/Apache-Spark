package com.apache.spark.infrastructure;

import static com.apache.spark.domain.VehicleCsvPositions.ACCELERATION;
import static com.apache.spark.domain.VehicleCsvPositions.DISPLACEMENT;
import static com.apache.spark.domain.VehicleCsvPositions.MPG;
import static com.apache.spark.domain.VehicleCsvPositions.WEIGHT;

import java.util.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;

public class LabelPointAssembler implements Function<JavaRDD<Row>, JavaRDD<LabeledPoint>> {

  public JavaRDD<LabeledPoint> apply(JavaRDD<Row> cleansedData) {
    return cleansedData.map(row ->
        new LabeledPoint(row.getDouble(MPG),
            Vectors.dense(row.getDouble(DISPLACEMENT),
                row.getDouble(WEIGHT),
                row.getDouble(ACCELERATION))));
  }

}
