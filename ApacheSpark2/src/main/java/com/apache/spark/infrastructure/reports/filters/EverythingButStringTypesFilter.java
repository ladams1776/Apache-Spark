package com.apache.spark.infrastructure.reports.filters;

import static org.apache.spark.sql.types.DataTypes.StringType;

import java.util.function.Function;
import org.apache.spark.sql.types.StructField;

public class EverythingButStringTypesFilter implements Function<StructField, Boolean> {

  @Override
  public Boolean apply(StructField structField) {
    return !structField.dataType().equals(StringType);
  }
}
