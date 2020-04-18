package com.apache.spark.infrastructure.kmeans;

import com.apache.spark.domain.kmeans.Auto;
import com.apache.spark.domain.shared.CleanseData;
import com.apache.spark.infrastructure.shared.AbstractDataCleanser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

public class CarCleaner extends AbstractDataCleanser implements CleanseData<Dataset<Row>> {

    public CarCleaner(SparkSession sparkSession) {
        super(sparkSession);
    }

    @Override
    public Dataset<Row> apply(Dataset<Row> data) {
        final JavaRDD<Row> repartitionAutoDF = data.toJavaRDD().repartition(2);

        final JavaRDD<Row> cleansedData = repartitionAutoDF.map(row -> {
            final double doors = row.getString(3).equals("two") ? 1.0 : 2.0;
            final double body = row.getString(4).equals("sedan") ? 1.0 : 2.0;

            return RowFactory.create(doors, body,
                    Double.valueOf(row.getString(7)),
                    Double.valueOf(row.getString(8)),
                    Double.valueOf(row.getString(9)));
        });

        final Dataset<Row> cleansedCarData = this.sparkSession.createDataFrame(cleansedData, Auto.SCHEMA);

        System.out.println("Cleansed Data: ");
        cleansedCarData.show();
        System.out.println("Cleansed Data Schema: ");
        cleansedCarData.printSchema();

        return cleansedCarData;
    }
}
