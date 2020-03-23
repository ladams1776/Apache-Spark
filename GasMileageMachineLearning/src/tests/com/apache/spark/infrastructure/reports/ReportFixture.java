package com.apache.spark.infrastructure.reports;

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ReportFixture {

  public final static StructField MPG_DOUBLE_STRUCT_FIELD = createStructField("MPG", DoubleType,
      false);

  public final static StructField MPG_STRING_STRUCT_FIELD = createStructField("MPG", StringType,
      false);

  public final static StructType AUTOMOBILE_SCHEMA = createStructType(
      new StructField[]{
          createStructField("MPG", DoubleType, false),
          createStructField("CYLINDERS", DoubleType, false),
          createStructField("DISPLACEMENT", DoubleType, false),
          createStructField("HP", DoubleType, false),
          createStructField("WEIGHT", DoubleType, false),
          createStructField("ACCELERATION", DoubleType, false),
          createStructField("MODELYEAR", DoubleType, false),
          createStructField("NAME", StringType, false),
      });

  public final static StructType AUTOMOBILE_MPG_STRUCT = createStructType(
      new StructField[]{MPG_STRING_STRUCT_FIELD});


  public final static Row AUTOMOBILE_ROW = RowFactory.create(
      "18", "8", "307", "130", "3504", "12",
      "70", "Chevy");
}
