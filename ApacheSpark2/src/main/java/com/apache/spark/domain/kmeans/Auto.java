package com.apache.spark.domain.kmeans;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Auto {
    public final static StructType SCHEMA = DataTypes
            .createStructType(new StructField[]{
                    DataTypes.createStructField("DOORS", DataTypes.DoubleType, false),
                    DataTypes.createStructField("BODY", DataTypes.DoubleType, false),
                    DataTypes.createStructField("HP", DataTypes.DoubleType, false),
                    DataTypes.createStructField("RPM", DataTypes.DoubleType, false),
                    DataTypes.createStructField("MPG", DataTypes.DoubleType, false)
            });
}
