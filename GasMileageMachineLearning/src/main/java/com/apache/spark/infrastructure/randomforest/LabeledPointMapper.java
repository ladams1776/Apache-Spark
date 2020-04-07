package com.apache.spark.infrastructure.randomforest;

import static com.apache.spark.domain.randomforest.Bank.AGE;
import static com.apache.spark.domain.randomforest.Bank.BALANCE;
import static com.apache.spark.domain.randomforest.Bank.CONTACT;
import static com.apache.spark.domain.randomforest.Bank.DAY;
import static com.apache.spark.domain.randomforest.Bank.DEFAULT;
import static com.apache.spark.domain.randomforest.Bank.EDUCATION;
import static com.apache.spark.domain.randomforest.Bank.HOUSING;
import static com.apache.spark.domain.randomforest.Bank.JOB;
import static com.apache.spark.domain.randomforest.Bank.LOAN;
import static com.apache.spark.domain.randomforest.Bank.MARITAL;
import static com.apache.spark.domain.randomforest.Bank.MONTH;

import com.apache.spark.domain.randomforest.BankLabelMapper;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Row;

public class LabeledPointMapper implements BankLabelMapper<Row, LabeledPoint> {

  public LabeledPoint map(Row row) {
    return new LabeledPoint(row.getDouble(AGE), Vectors.dense(
        row.getDouble(JOB),
        row.getDouble(MARITAL),
        row.getDouble(EDUCATION),
        row.getDouble(DEFAULT),
        row.getDouble(BALANCE),
        row.getDouble(HOUSING),
        row.getDouble(LOAN),
        row.getDouble(CONTACT),
        row.getDouble(DAY),
        row.getDouble(MONTH)
    ));
  }

}
