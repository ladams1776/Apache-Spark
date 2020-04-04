package com.apache.spark.infrastructure.randomforest;

import com.apache.spark.domain.randomforest.BankIndicatorMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class IndicatorMapper implements BankIndicatorMapper<Row> {

  private static final int AGE = 0;
  private static final int MARITAL = 2;
  private static final int EDUCATION = 3;
  private static final int DEFAULT = 4;
  private static final int BALANCE = 5;
  private static final int LOAN = 7;
  private static final int OUTCOME = 16;

  /**
   * We need to massage the data to be able to process it in Machine Learning. So we are creating
   * 'indicator variables' for the different possible values on columns we want to use as our
   * selected feature variables.
   *
   * @param row the row we want to check the values of to return a new row
   * @return a new row, with the values all converted to values of Double Type.
   */
  @Override
  public Row map(Row row) throws Exception {
    // Convert age to double
    final Double age = Double.valueOf(row.getString(AGE));

    // convert outcome to double
    final double outcome = row.getString(OUTCOME).equals("yes") ? 1.0 : 0.0;

    // create indicator variable for marital status
    final double single = row.getString(MARITAL).equals("single") ? 1.0 : 0.0;
    final double married = row.getString(MARITAL).equals("married") ? 1.0 : 0.0;
    final double divorced = row.getString(MARITAL).equals("divorced") ? 1.0 : 0.0;

    // create indicator variables for education
    final double primary = row.getString(EDUCATION).equals("primary") ? 1.0 : 0.0;
    final double secondary = row.getString(EDUCATION).equals("secondary") ? 1.0 : 0.0;
    final double tertiary = row.getString(EDUCATION).equals("tertiary") ? 1.0 : 0.0;

    // Convert default to double
    final double isDefault = row.getString(DEFAULT).equals("yes") ? 1.0 : 0.0;

    // Convert balance to double
    final Double balance = Double.valueOf(row.getString(BALANCE));

    // Convert loan to double
    final double loan = row.getString(LOAN).equals("yes") ? 1.0 : 0.0;

    return RowFactory
        .create(outcome, age, single, married, divorced, primary, secondary, tertiary, isDefault,
            balance, loan);
  }
}
