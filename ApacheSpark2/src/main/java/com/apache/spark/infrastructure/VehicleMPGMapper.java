package com.apache.spark.infrastructure;


import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.ACCELERATION;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.CYLINDERS;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.DISPLACEMENT;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.HORSE_POWER;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.MODELYEAR;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.MPG;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.NAME;
import static com.apache.spark.domain.linearRegression.VehicleCsvPositions.WEIGHT;
import static java.lang.Double.valueOf;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class VehicleMPGMapper implements
    java.util.function.Function<Broadcast<Double>, Function<Row, Row>> {

  public Function<Row, Row> apply(Broadcast<Double> horsePowerFiller) {
    return row -> {
      final Double horsePower = row.getString(HORSE_POWER).equals("?")
          ? horsePowerFiller.value()
          : valueOf(row.getString(HORSE_POWER));

      return RowFactory.create(
          valueOf(row.getString(MPG)),
          valueOf(row.getString(CYLINDERS)),
          valueOf(row.getString(DISPLACEMENT)),
          horsePower,
          valueOf(row.getString(WEIGHT)),
          valueOf(row.getString(ACCELERATION)),
          valueOf(row.getString(MODELYEAR)),
          row.getString(NAME)
      );
    };
  }
}
