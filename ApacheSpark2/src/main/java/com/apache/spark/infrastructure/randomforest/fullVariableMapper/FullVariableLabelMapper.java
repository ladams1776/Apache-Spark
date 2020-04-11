package com.apache.spark.infrastructure.randomforest.fullVariableMapper;

import com.apache.spark.domain.randomforest.BankLabelMapper;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;

public class FullVariableLabelMapper implements BankLabelMapper<Row, LabeledPoint> {

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
        row.getDouble(10),
        row.getDouble(11),
        row.getDouble(12),
        row.getDouble(13),
        row.getDouble(14),
        row.getDouble(15),
        row.getDouble(16),
        row.getDouble(17),
        row.getDouble(18),
        row.getDouble(19),
        row.getDouble(20),
        row.getDouble(21),
        row.getDouble(22)
    ));
  }

}
