package com.apache.spark.infrastructure.randomforest;

import com.apache.spark.domain.randomforest.OutcomeCorrelationReport;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class CorrelationReport implements OutcomeCorrelationReport<StructType, Dataset<Row>> {

  /**
   * Print the correlation between the Bank fields and the target field (OUTCOME field)
   * @param bankSchema The schema of the cleansedBanks we are passing in
   * @param cleansedBanks the dataset that has all our feature and target fields.
   */
  @Override
  public void print(StructType bankSchema, Dataset<Row> cleansedBanks) {
    Stream.of(bankSchema.fields())
        .filter(field -> !field.dataType().equals(DataTypes.StringType))
        .filter(field -> !field.name().equals("OUTCOME"))
        .forEach(field -> {
          final String fieldName = field.name();
          final double outcome = cleansedBanks.stat().corr("OUTCOME", fieldName);
          final String message = "Correlation between OUTCOME and "
              .concat(fieldName)
              .concat(" is ")
              .concat(String.valueOf(outcome));

          System.out.println(message);
        });
  }
}
