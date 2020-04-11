package com.apache.spark.infrastructure;

import static com.apache.spark.domain.VehicleCsvPositions.ACCELERATION;
import static com.apache.spark.domain.VehicleCsvPositions.CYLINDERS;
import static com.apache.spark.domain.VehicleCsvPositions.DISPLACEMENT;
import static com.apache.spark.domain.VehicleCsvPositions.HORSE_POWER;
import static com.apache.spark.domain.VehicleCsvPositions.MODELYEAR;
import static com.apache.spark.domain.VehicleCsvPositions.MPG;
import static com.apache.spark.domain.VehicleCsvPositions.NAME;
import static com.apache.spark.domain.VehicleCsvPositions.WEIGHT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.apache.spark.infrastructure.VehicleMPGMapper;
import java.util.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.Test;
import org.mockito.Mockito;

public class VehicleMPGMapperUnitTest {

  @Test
  public void apply_withNodirtyDataInHorsePower_willCorrectHorsePower() throws Exception {
    final Row row = RowFactory.create(
        "18", "8", "307", "130", "3504", "12",
        "70", "Chevy");

    Function<Row, Row> actual = new VehicleMPGMapper()
        .apply((Broadcast<Double>) mock(Broadcast.class));
    Row call = actual.call(row);

    assertEquals(Optional.of(18.0), Optional.of(call.getDouble(MPG)));
    assertEquals(Optional.of(8.0), Optional.of(call.getDouble(CYLINDERS)));
    assertEquals(Optional.of(307.0), Optional.of(call.getDouble(DISPLACEMENT)));
    assertEquals(Optional.of(130.0), Optional.of(call.getDouble(HORSE_POWER)));
    assertEquals(Optional.of(3504.0), Optional.of(call.getDouble(WEIGHT)));
    assertEquals(Optional.of(12.0), Optional.of(call.getDouble(ACCELERATION)));
    assertEquals(Optional.of(70.0), Optional.of(call.getDouble(MODELYEAR)));
    assertEquals(Optional.of("Chevy"), Optional.of(call.getString(NAME)));
  }

  @Test
  public void apply_withDirtyDataInHorsePower_willCorrectHorsePower() throws Exception {
    final Broadcast<Double> mockBroadcast = mock(Broadcast.class);
    double expected = 100.0;
    Mockito.when(mockBroadcast.value()).thenReturn(expected);
    final Row row = RowFactory.create(
        "18", "8", "307", "?", "3504", "12",
        "70", "Chevy");

    Function<Row, Row> actual = new VehicleMPGMapper().apply(mockBroadcast);
    Row call = actual.call(row);

    assertEquals(Optional.of(18.0), Optional.of(call.getDouble(MPG)));
    assertEquals(Optional.of(8.0), Optional.of(call.getDouble(CYLINDERS)));
    assertEquals(Optional.of(307.0), Optional.of(call.getDouble(DISPLACEMENT)));
    assertEquals(Optional.of(expected), Optional.of(call.getDouble(HORSE_POWER)));
    assertEquals(Optional.of(3504.0), Optional.of(call.getDouble(WEIGHT)));
    assertEquals(Optional.of(12.0), Optional.of(call.getDouble(ACCELERATION)));
    assertEquals(Optional.of(70.0), Optional.of(call.getDouble(MODELYEAR)));
    assertEquals(Optional.of("Chevy"), Optional.of(call.getString(NAME)));
  }
}