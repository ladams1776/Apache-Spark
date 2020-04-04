package com.apache.spark.application;

import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Logger.getLogger;

import com.apache.spark.domain.randomforest.BankIndicatorMapper;
import com.apache.spark.domain.randomforest.OutcomeCorrelationReport;
import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import com.apache.spark.infrastructure.randomforest.CorrelationReport;
import com.apache.spark.infrastructure.randomforest.IndicatorMapper;
import java.util.stream.Stream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main_RandomForest {

  private final static SparkConnection sparkConnection = new SparkConnectionBuilder().build();

  private final static BankIndicatorMapper<Row> indicatorMapper = new IndicatorMapper();
  private final static OutcomeCorrelationReport<StructType, Dataset<Row>> correlationReport = new CorrelationReport();

  public static void main(String[] args) {
    getLogger("org").setLevel(ERROR);
    getLogger("akka").setLevel(ERROR);

    // ******************** Setup ************************************* //
    final SparkSession sparkSession = sparkConnection.getSparkSession();

    final Dataset<Row> banks = sparkSession
        .read()
        .option("header", "true")
        .option("sep", ";")
        .csv("src/main/resources/bank.csv");

    banks.show(5);
    banks.printSchema();

    // ******************** Cleanse Data ************************************* //
    // Convert all data types as double;
    // Use indicator variables
    final StructType bankSchema = DataTypes.createStructType(new StructField[]{
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

    // Change DF to RDD. To make indicator variables (map the values over).
    final JavaRDD<Row> paritionedBanks = banks.toJavaRDD().repartition(2);

    // There is a bug with Apache spark apparently. I can't do a method reference here ☹️.
    final JavaRDD<Row> indicatedVariables = paritionedBanks.map(row -> indicatorMapper.map(row));

    // Change RDD back to DFR
    final Dataset<Row> cleansedBanks = sparkSession.createDataFrame(indicatedVariables, bankSchema);
    System.out
        .println("Transformed/Cleansed data - applying indicator variables to the Bank Schema");
    cleansedBanks.show(10);
    cleansedBanks.printSchema();

    // ******************** Analyze Data ************************************* //
    correlationReport.print(bankSchema, cleansedBanks);

    // ******************** Prepare for Machine Learning ********************* //
    //@TODO: Left off here
  }

}
