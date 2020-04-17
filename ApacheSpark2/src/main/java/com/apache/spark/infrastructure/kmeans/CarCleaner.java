package com.apache.spark.infrastructure.kmeans;

import com.apache.spark.domain.shared.CleanseData;
import com.apache.spark.infrastructure.shared.AbstractDataCleanser;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CarCleaner extends AbstractDataCleanser implements CleanseData<Dataset<Row>> {

    public CarCleaner(SparkSession sparkSession) {
        super(sparkSession);
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> data) {
        //@TODO: Fill me in - cleanse the data
        return null;
    }
}
