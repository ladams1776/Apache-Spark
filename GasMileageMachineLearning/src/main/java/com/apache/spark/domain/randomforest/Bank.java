package com.apache.spark.domain.randomforest;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Bank {

  public static final int AGE = 0;
  public static final int JOB = 1;
  public static final int MARITAL = 2;
  public static final int EDUCATION = 3;
  public static final int DEFAULT = 4;
  public static final int BALANCE = 5;
  public static final int HOUSING = 6;
  public static final int LOAN = 7;
  public static final int CONTACT = 8;
  public static final int DAY = 9;
  public static final int MONTH = 10;

  public static final int OUTCOME = 16;


  public static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
      DataTypes.createStructField("OUTCOME", DataTypes.DoubleType, false),
      DataTypes.createStructField("AGE", DataTypes.DoubleType, false),
      DataTypes.createStructField("SINGLE", DataTypes.DoubleType, false),
      DataTypes.createStructField("MARRIED", DataTypes.DoubleType, false),
      DataTypes.createStructField("DIVORCED", DataTypes.DoubleType, false),
      DataTypes.createStructField("PRIMARY", DataTypes.DoubleType, false),
      DataTypes.createStructField("SECONDARY", DataTypes.DoubleType, false),
      DataTypes.createStructField("TERTIARY", DataTypes.DoubleType, false),
      DataTypes.createStructField("DEFAULT", DataTypes.DoubleType, false),
      DataTypes.createStructField("BALANCE", DataTypes.DoubleType, false),
      DataTypes.createStructField("LOAN", DataTypes.DoubleType, false),
  });
}
