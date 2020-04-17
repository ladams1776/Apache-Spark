package com.apache.spark.infrastructure.randomforest;

import com.apache.spark.domain.shared.CleanseData;
import com.apache.spark.domain.shared.VariableIndicatorMapper;
import com.apache.spark.infrastructure.randomforest.fullVariableMapper.FullVariableMapper;
import com.apache.spark.infrastructure.shared.AbstractDataCleanser;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class CleanseBanks extends AbstractDataCleanser implements CleanseData<Dataset<Row>> {
    //  private final static BankVariableIndicatorMapper<Row, StructType> indicatorMapper = new MaritalEducationVariableMapper();
    private final VariableIndicatorMapper<Row, StructType> indicatorMapper = new FullVariableMapper();

    public CleanseBanks(SparkSession sparkSession) {
        super(sparkSession);
    }

    /**
     * Convert all data types as double;
     * Use indicator variables
     * Change DF to RDD. To make indicator variables (map the values over).
     * There is a bug with Apache spark apparently. I can't do a method reference here
     *
     * @param banks
     * @return
     */
    @Override
    public Dataset<Row> apply(Dataset<Row> banks) {

        final JavaRDD<Row> indicatedVariables = banks.toJavaRDD()
                .repartition(2)
                .map(row -> indicatorMapper.map(row));

        // Change RDD back to DFR
        final Dataset<Row> cleansedBanks =
                this.sparkSession.createDataFrame(indicatedVariables, indicatorMapper.getSchema());

        System.out
                .println("Transformed/Cleansed data - applying indicator variables to the Bank Schema");
        cleansedBanks.show(10);
        cleansedBanks.printSchema();

        return cleansedBanks;
    }
}
