package com.apache.spark.infrastructure.reports.filters;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.spark.sql.types.StructField;
import org.junit.Test;

public class EverythingButStringTypesFilterUnitTest {

  private final EverythingButStringTypesFilter test =
      new EverythingButStringTypesFilter();

  @Test
  public void apply_withDoubleField_willReturnTrue() {
    final StructField nonStringField = createStructField("MPG", DoubleType, false);
    assertTrue(test.apply(nonStringField));
  }

  @Test
  public void apply_withStringField_willReturnFalse() {
    final StructField stringField = createStructField("MPG", StringType, false);
    assertFalse(test.apply(stringField));
  }
}