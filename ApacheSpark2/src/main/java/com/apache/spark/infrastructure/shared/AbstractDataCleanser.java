package com.apache.spark.infrastructure.shared;

import org.apache.spark.sql.SparkSession;

abstract public class AbstractDataCleanser {
    protected final SparkSession sparkSession;

    public AbstractDataCleanser(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

}
