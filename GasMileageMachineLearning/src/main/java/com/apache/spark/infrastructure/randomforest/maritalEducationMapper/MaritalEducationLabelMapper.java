package com.apache.spark.infrastructure.randomforest.maritalEducationMapper;

import com.apache.spark.domain.randomforest.BankLabelMapper;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;

public class MaritalEducationLabelMapper implements BankLabelMapper<Row, LabeledPoint> {

  /**
   * Needs to have the same count of rows as the MaritalEducationMapperVariable.
   *
   * @param row
   * @return
   */
  public LabeledPoint map(Row row) {
    return new LabeledPoint(row.getDouble(0), Vectors.dense(
        row.getDouble(1),
        row.getDouble(2),
        row.getDouble(3),
        row.getDouble(4),
        row.getDouble(5),
        row.getDouble(6),
        row.getDouble(7),
        row.getDouble(8),
        row.getDouble(9),
        row.getDouble(10)
    ));
  }

}
