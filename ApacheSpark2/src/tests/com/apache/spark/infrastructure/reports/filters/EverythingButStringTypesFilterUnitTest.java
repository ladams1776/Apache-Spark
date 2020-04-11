package com.apache.spark.infrastructure.reports.filters;

import static com.apache.spark.infrastructure.reports.ReportFixture.MPG_DOUBLE_STRUCT_FIELD;
import static com.apache.spark.infrastructure.reports.ReportFixture.MPG_STRING_STRUCT_FIELD;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.apache.spark.infrastructure.reports.ReportFixture;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;

public class EverythingButStringTypesFilterUnitTest {

  private final EverythingButStringTypesFilter tester =
      new EverythingButStringTypesFilter();

  @Test
  public void apply_withDoubleField_willReturnTrue() {
    final StructField nonStringField = MPG_DOUBLE_STRUCT_FIELD;
    assertTrue(tester.apply(nonStringField));
  }

  @Test
  public void apply_withStringField_willReturnFalse() {
    final StructField stringField = MPG_STRING_STRUCT_FIELD;
    assertFalse(tester.apply(stringField));
  }
}