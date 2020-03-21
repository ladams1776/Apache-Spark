package com.apache.spark.infrastructure;


import static com.apache.spark.domain.VehicleCsvPositions.ACCELERATION;
import static com.apache.spark.domain.VehicleCsvPositions.CYLINDERS;
import static com.apache.spark.domain.VehicleCsvPositions.DISPLACEMENT;
import static com.apache.spark.domain.VehicleCsvPositions.HORSE_POWER;
import static com.apache.spark.domain.VehicleCsvPositions.MODELYEAR;
import static com.apache.spark.domain.VehicleCsvPositions.MPG;
import static com.apache.spark.domain.VehicleCsvPositions.NAME;
import static com.apache.spark.domain.VehicleCsvPositions.WEIGHT;

import java.util.function.BiFunction;
import org.apache.spark.api.java.JavaRDD;
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
          : Double.valueOf(row.getString(HORSE_POWER));

      return RowFactory.create(
          Double.valueOf(row.getString(MPG)),
          Double.valueOf(row.getString(CYLINDERS)),
          Double.valueOf(row.getString(DISPLACEMENT)),
          horsePower,
          Double.valueOf(row.getString(WEIGHT)),
          Double.valueOf(row.getString(ACCELERATION)),
          Double.valueOf(row.getString(MODELYEAR)),
          Double.valueOf(row.getString(NAME))
      );
    };
  }
}
