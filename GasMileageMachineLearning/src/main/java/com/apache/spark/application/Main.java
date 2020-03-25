package com.apache.spark.application;

import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Logger.getLogger;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.apache.spark.sql.types.DataTypes.createStructType;

import com.apache.spark.domain.reports.CorrelationMPGReport;
import com.apache.spark.infrastructure.LabelPointAssembler;
import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import com.apache.spark.infrastructure.VehicleMPGMapper;
import com.apache.spark.infrastructure.reports.SystemPrintCorrelationMPGReport;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main {

  private final static SparkConnection sparkConnection = new SparkConnectionBuilder().build();
  private final static VehicleMPGMapper vehicleMPGMapper = new VehicleMPGMapper();
  private final static LabelPointAssembler labelPointAssembler = new LabelPointAssembler();
  private final static CorrelationMPGReport<StructType, Dataset<Row>> mpgCorrelationReport = new SystemPrintCorrelationMPGReport();
  private final static Double HORSE_POWER_DEFAULT = 80.0;

  public static void main(String... args) {
    getLogger("org").setLevel(ERROR);
    getLogger("akka").setLevel(ERROR);

    // ******************** Setup ************************************* //
    System.out.println("Working");
    final JavaSparkContext spContext = sparkConnection.getSpContext();
    final SparkSession sparkSession = sparkConnection.getSparkSession();

    // ******************** Load the data ***************************** //
    final Dataset<Row> autoDF = sparkSession.read()
        .option("header", "true")
        .csv("src/main/resources/auto-miles-per-gallon.csv");

    autoDF.show(5);
    autoDF.printSchema();

    // ************************* Cleanse Data ************************** //

    // Convert all data types as double; change missing values to standard ones.
    final StructType autoSchema = createStructType(
        new StructField[]{
            createStructField("MPG", DoubleType, false),
            createStructField("CYLINDERS", DoubleType, false),
            createStructField("DISPLACEMENT", DoubleType, false),
            createStructField("HP", DoubleType, false),
            createStructField("WEIGHT", DoubleType, false),
            createStructField("ACCELERATION", DoubleType, false),
            createStructField("MODELYEAR", DoubleType, false),
            createStructField("NAME", StringType, false),
        });

    final Broadcast<Double> horsePowerFiller = getBroadCast(spContext, HORSE_POWER_DEFAULT);

    // Change data frame back to RDD, so we can stub in horsePowerFiller for values with '?'
    // This is the actual cleaning of Data.
    final JavaRDD<Row> rdd1 = autoDF.toJavaRDD().repartition(2);
    final JavaRDD<Row> cleanedRDD = rdd1.map(vehicleMPGMapper.apply(horsePowerFiller));

    // Create Data Frame back.
    Dataset<Row> autoCleansed = sparkSession.createDataFrame(cleanedRDD, autoSchema);
    System.out.println("Transformed Data : ");
    autoCleansed.show(5);

    // ************************* Analyze Data ************************** //

    // Perform correlation analysis
    mpgCorrelationReport.report(autoSchema, autoCleansed);

    // ************************* Prepare for Machine Learning **************** //
    // convert data to labeled Point structure
    final JavaRDD<Row> repartitionedAutoCleansed = autoCleansed.toJavaRDD().repartition(2);

    final JavaRDD<LabeledPoint> labelPoint = labelPointAssembler.apply(repartitionedAutoCleansed);

    final Dataset<Row> autoLabeledPoint = sparkSession
        .createDataFrame(labelPoint, LabeledPoint.class);

    autoLabeledPoint.show(5);

    // Split the data into training and test sets (10% held out for testing, 90% for training).
    final Dataset<Row>[] splits = autoLabeledPoint.randomSplit(new double[]{0.9, 0.1});

    final Dataset<Row> trainingData = splits[0];
    final Dataset<Row> testingData = splits[1];

    // ************************* Perform Machine Learning **************** //

    // create the LinearRegression model
    final LinearRegression linearRegression = new LinearRegression();
    // create the model
    final LinearRegressionModel linearRegressionModel = linearRegression.fit(trainingData);

    // print out coefficients and intercept for Linear Regression
    System.out.println(
        "Coefficients: " + linearRegressionModel.coefficients() + " Intercept: "
            + linearRegressionModel.intercept());

    // predict on the test data
    final Dataset<Row> predictions = linearRegressionModel.transform(testingData);

    // view results
    predictions.select("label", "prediction", "features").show();

    // Compute R2 for the model on test data.
    final RegressionEvaluator regressionEvaluator = new RegressionEvaluator()
        .setLabelCol("label")
        .setPredictionCol("prediction")
        .setMetricName("r2");

    final double evaluate = regressionEvaluator.evaluate(predictions);
    System.out.println("R2 on test data = " + evaluate);

    hold();

  }

  public static void hold() {
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  /**
   * Broadcast the default value for Horse Power - b/c sometimes there is a '?' (dirty data)
   * <p>
   * According to docs: A broadcast variable. Broadcast variables allow the programmer to keep a
   * read-only variable cached on each machine rather than shipping a copy of it with tasks. They
   * can be used, for example, to give every node a copy of a large input dataset in an efficient
   * manner. Spark also attempts to distribute broadcast variables using efficient broadcast
   * algorithms to reduce communication cost.
   * </p>
   *
   * @param sparkContext
   * @param value
   * @return
   */
  private static Broadcast<Double> getBroadCast(JavaSparkContext sparkContext, Double value) {
    return sparkContext.broadcast(value);
  }
}
