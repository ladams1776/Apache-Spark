package com.apache.spark.infrastructure.randomforest.fullVariableMapper;

import static com.apache.spark.domain.randomforest.Bank.AGE;
import static com.apache.spark.domain.randomforest.Bank.BALANCE;
import static com.apache.spark.domain.randomforest.Bank.CAMPAIGN;
import static com.apache.spark.domain.randomforest.Bank.DEFAULT;
import static com.apache.spark.domain.randomforest.Bank.EDUCATION;
import static com.apache.spark.domain.randomforest.Bank.JOB;
import static com.apache.spark.domain.randomforest.Bank.LOAN;
import static com.apache.spark.domain.randomforest.Bank.MARITAL;
import static com.apache.spark.domain.randomforest.Bank.OUTCOME;
import static com.apache.spark.domain.randomforest.Bank.PDAYS;
import static com.apache.spark.domain.randomforest.Bank.PREVIOUS;
import static com.apache.spark.domain.randomforest.Bank.PREVIOUS_OUTCOME;

import com.apache.spark.domain.randomforest.BankVariableIndicatorMapper;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FullVariableMapper implements BankVariableIndicatorMapper<Row, StructType> {

  public static final StructType CORRESPONDING_SCHEMA = DataTypes
      .createStructType(new StructField[]{
          DataTypes.createStructField("OUTCOME", DataTypes.DoubleType, false),
          DataTypes.createStructField("IS_PREV_FAILURE", DataTypes.DoubleType, false),
          DataTypes.createStructField("IS_PREV_OTHER", DataTypes.DoubleType, false),
          DataTypes.createStructField("IS_PREV_UNKNOWN", DataTypes.DoubleType, false),
          DataTypes.createStructField("AGE", DataTypes.DoubleType, false),
          DataTypes.createStructField("SINGLE", DataTypes.DoubleType, false),
          DataTypes.createStructField("MARRIED", DataTypes.DoubleType, false),
          DataTypes.createStructField("DIVORCED", DataTypes.DoubleType, false),
          DataTypes.createStructField("PRIMARY", DataTypes.DoubleType, false),
          DataTypes.createStructField("SECONDARY", DataTypes.DoubleType, false),
          DataTypes.createStructField("TERTIARY", DataTypes.DoubleType, false),
          DataTypes.createStructField("DEFAULT", DataTypes.DoubleType, false),
          DataTypes.createStructField("BALANCE", DataTypes.DoubleType, false),
          DataTypes.createStructField("UNEMPLOYED", DataTypes.DoubleType, false),
          DataTypes.createStructField("SERVICES", DataTypes.DoubleType, false),
          DataTypes.createStructField("MANAGEMENT", DataTypes.DoubleType, false),
          DataTypes.createStructField("BLUE_COLLAR", DataTypes.DoubleType, false),
          DataTypes.createStructField("SELF_EMPLOYED", DataTypes.DoubleType, false),
          DataTypes.createStructField("TECHNICIAN", DataTypes.DoubleType, false),
          DataTypes.createStructField("ENTREPRENEUR", DataTypes.DoubleType, false),
          DataTypes.createStructField("ADMIN", DataTypes.DoubleType, false),
          DataTypes.createStructField("HOUSEMAID", DataTypes.DoubleType, false),
          DataTypes.createStructField("RETIRED", DataTypes.DoubleType, false),

          DataTypes.createStructField("CAMPAIGN", DataTypes.DoubleType, false),
          DataTypes.createStructField("PDAYS", DataTypes.DoubleType, false),
          DataTypes.createStructField("PREVIOUS", DataTypes.DoubleType, false),

      });


  /**
   * The Schema that corresponds to the row we are returning in `map()` method.
   * @return
   */
  @Override
  public StructType getSchema() {
    return CORRESPONDING_SCHEMA;
  }

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

    // Create indicator variables for previous outcome column.
    final double isPreviousFailure = row.getString(PREVIOUS_OUTCOME).equals("failure") ? 1.0 : 0.0;
    final double isPreviousOther = row.getString(PREVIOUS_OUTCOME).equals("other") ? 1.0 : 0.0;
    final double isPreviousUnknown = row.getString(PREVIOUS_OUTCOME).equals("unknown") ? 1.0 : 0.0;

    // Create indicator variables for Job column
    final double unemployed = row.getString(JOB).equals("unemployed") ? 1.0 : 0.0;
    final double services = row.getString(JOB).equals("services") ? 1.0 : 0.0;
    final double management = row.getString(JOB).equals("management") ? 1.0 : 0.0;
    final double blueCollar = row.getString(JOB).equals("blue-collar") ? 1.0 : 0.0;
    final double selfEmployed = row.getString(JOB).equals("self-employed") ? 1.0 : 0.0;
    final double technician = row.getString(JOB).equals("technician") ? 1.0 : 0.0;
    final double entrepreneur = row.getString(JOB).equals("entrepreneur") ? 1.0 : 0.0;
    final double admin = row.getString(JOB).equals("admin.") ? 1.0 : 0.0;
    final double housemaid = row.getString(JOB).equals("housemaid") ? 1.0 : 0.0;
    final double retired = row.getString(JOB).equals("retired") ? 1.0 : 0.0;

    // Convert balance to double
    final Double balance = Double.valueOf(row.getString(BALANCE));
    // Convert loan to double
    final double loan = row.getString(LOAN).equals("yes") ? 1.0 : 0.0;
    // Convert campaign to double
    final Double campaign = Double.valueOf(row.getString(CAMPAIGN));
    // Convert pdays to double
    final Double pdays = Double.valueOf(row.getString(PDAYS));
    // Convert previous to double
    final Double previous = Double.valueOf(row.getString(PREVIOUS));

    return RowFactory
        .create(outcome, age, isDefault, balance, loan, campaign, pdays, previous,
            single, married, divorced, // marital status
            primary, secondary, tertiary,  // education status
            isPreviousFailure, isPreviousOther, isPreviousUnknown, //poutcome
            //employment status
            unemployed, services, management, blueCollar, selfEmployed, technician, entrepreneur,
            admin, housemaid, retired);
  }

}
