package com.apache.spark.infrastructure.randomforest.maritalEducationMapper;

import static com.apache.spark.domain.randomforest.BankPositions.AGE;
import static com.apache.spark.domain.randomforest.BankPositions.BALANCE;
import static com.apache.spark.domain.randomforest.BankPositions.DEFAULT;
import static com.apache.spark.domain.randomforest.BankPositions.EDUCATION;
import static com.apache.spark.domain.randomforest.BankPositions.LOAN;
import static com.apache.spark.domain.randomforest.BankPositions.MARITAL;
import static com.apache.spark.domain.randomforest.BankPositions.OUTCOME;

import com.apache.spark.domain.shared.VariableIndicatorMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * First mapper I used to try to do correlation analysis on. It was not accurate enough.
 */
public class MaritalEducationVariableMapper implements VariableIndicatorMapper<Row, StructType> {

  public static final StructType CORRESPONDING_SCHEMA = DataTypes
      .createStructType(new StructField[]{
          DataTypes.createStructField("OUTCOME", DataTypes.DoubleType, false),
          DataTypes.createStructField("AGE", DataTypes.DoubleType, false),
          DataTypes.createStructField("SINGLE", DataTypes.DoubleType, false),
          DataTypes.createStructField("MARRIED", DataTypes.DoubleType, false),
          DataTypes.createStructField("DIVORCED", DataTypes.DoubleType, false),
          DataTypes.createStructField("PRIMARY", DataTypes.DoubleType, false),
          DataTypes.createStructField("SECONDARY", DataTypes.DoubleType, false),
          DataTypes.createStructField("TERTIARY", DataTypes.DoubleType, false),
          DataTypes.createStructField("DEFAULT", DataTypes.DoubleType, false),
          DataTypes.createStructField("BALANCE", DataTypes.DoubleType, false),
          DataTypes.createStructField("LOAN", DataTypes.DoubleType, false),
      });


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

    // Target/Label variable:
    // convert outcome to double
    final double outcome = row.getString(OUTCOME).equals("yes") ? 1.0 : 0.0;

    // Feature variables:
    // Convert age to double
    final Double age = Double.valueOf(row.getString(AGE));

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

  @Override
  public StructType getSchema() {
    return CORRESPONDING_SCHEMA;
  }
}
