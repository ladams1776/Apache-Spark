package com.apache.spark.application;

import static org.apache.spark.sql.functions.col;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import com.apache.spark.infrastructure.reports.filters.EverythingButStringTypesFilter;
import java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main_DecisionTree {

  private static final SparkConnection sparkConnection = new SparkConnectionBuilder().build();
  private static final SparkSession sparkSession = sparkConnection.getSparkSession();
  private static final JavaSparkContext sparkContext = sparkConnection.getSpContext();

  public static void main(String[] args) {
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    System.out.println("is Working");

    final Dataset<Row> iris = sparkSession.read()
        .option("header", "true")
        .csv("src/main/resources/iris.csv");

    System.out.println("Print the Schema of the iris.csv \n");
    iris.printSchema();
    System.out.println("Raw data");
    iris.show(5);

    // Declare the schema, we will use that to clean the data and also perform correlation analysis
    final StructType irisSchema = DataTypes
        .createStructType(new StructField[]{
            DataTypes.createStructField("SEPAL_LENGTH", DataTypes.DoubleType, false),
            DataTypes.createStructField("SEPAL_WIDTH", DataTypes.DoubleType, false),
            DataTypes.createStructField("PETAL_LENGTH", DataTypes.DoubleType, false),
            DataTypes.createStructField("PETAL_WIDTH", DataTypes.DoubleType, false),
            DataTypes.createStructField("SPECIES", DataTypes.StringType, false),
        });

    // Cleanse the data
    final Dataset<Row> cleanedIris = cleanseData(iris, irisSchema);
    // Analyze the data
    final Dataset<Row> indexedIris = analyzeData(cleanedIris, irisSchema);
    // Prepare for Machine Learning.
    final MachineLearningData machineLearningData = prepareForMachineLearning(indexedIris);
    // Perform Machine Learning.
    performMachineLearning(machineLearningData);

  }

  /**
   * Spark always has data stored in dataframes as strings, so we need to coerce them into the
   * correct types we want to use. In this case, all of our feature variables should be doubles and
   * our target variable is a string. So we are just converting here.
   *
   * @param iris
   * @param irisSchema
   * @return
   */
  private static Dataset<Row> cleanseData(Dataset<Row> iris, StructType irisSchema) {
    // Best for performance while converting. Only time we must play with a JavaRDD, is during the conversion of data types.
    final JavaRDD<Row> irisRepartitioned = iris.toJavaRDD().repartition(2);

    final JavaRDD<Row> valuesConverted = irisRepartitioned.map(row ->
        RowFactory.create(Double.valueOf(row.getString(0)),
            Double.valueOf(row.getString(1)),
            Double.valueOf(row.getString(2)),
            Double.valueOf(row.getString(3)),
            row.getString(4)));

    final Dataset<Row> irisCleansed = sparkSession.createDataFrame(valuesConverted, irisSchema);

    System.out.println("Cleaned Data (aka Transformed)");
    iris.show(5);

    return irisCleansed;
  }

  /**
   * We are choosing SPECIES as our 'target' variable - the variable we are going to compare other
   * variables (features) against, to figure out if we have correlations and how significant they
   * are.
   *
   * @param cleanedIris the dataframe we cleaned and want to analyze
   * @param irisSchema  the schema we can use to get the names for
   */
  private static Dataset<Row> analyzeData(Dataset<Row> cleanedIris, StructType irisSchema) {

    final Dataset<Row> indexedIris = addAnIndexColumn(cleanedIris);

    System.out.println("Correlation between feature variable fields and the target variable field");
    // Perform Correlation Analysis
    Arrays.stream(irisSchema.fields())
        .filter(field -> new EverythingButStringTypesFilter().apply(field))
        .forEach(field -> {
          final String info = "Correlation between IND_SPECIES and " + field.name() + " = ";
          final double fieldsCorrelationToIndex = indexedIris.stat()
              .corr("IND_SPECIES", field.name());

          System.out.println(info.concat(String.valueOf(fieldsCorrelationToIndex)));
        });

    return indexedIris;
  }

  /**
   * In order to make correlations against SPECIES field, we need the field to be in a numeric form.
   * So we are going to use a StringIndexer, to map SPECIES to IND_SPECIES, which will be the
   * numeric index column we can use, for correlation work.
   *
   * @param cleanedIris the data frame we want the additional numeric index on
   * @return the new data frame with the additional numeric index
   */
  private static Dataset<Row> addAnIndexColumn(Dataset<Row> cleanedIris) {
    final StringIndexer indexer = new StringIndexer()
        .setInputCol("SPECIES")
        .setOutputCol("IND_SPECIES");

    final StringIndexerModel siModel = indexer.fit(cleanedIris);
    final Dataset<Row> indexedIris = siModel.transform(cleanedIris);

    System.out
        .println("The Species Column, it's numeric index value, and the amount of rows out there");
    indexedIris.groupBy(col("SPECIES"), col("IND_SPECIES")).count().show();

    return indexedIris;
  }

  private static MachineLearningData prepareForMachineLearning(Dataset<Row> indexedIris) {
    final JavaRDD<Row> indexRepartitioned = indexedIris.toJavaRDD().repartition(2);

    final JavaRDD<LabeledPoint> dataReadyToBeSplit = indexRepartitioned.map(row ->
        new LabeledPoint(row.getDouble(5),
            Vectors.dense(row.getDouble(0),
                row.getDouble(1),
                row.getDouble(2),
                row.getDouble(3))));

    final Dataset<Row> featuresAndLabels = sparkSession
        .createDataFrame(dataReadyToBeSplit, LabeledPoint.class);

    System.out.println("Data with the Label Points");
    featuresAndLabels.show();

    final Dataset<Row>[] trainingAndTestDataSplit = featuresAndLabels
        .randomSplit(new double[]{0.7, 0.3});

    final Dataset<Row> trainingData = trainingAndTestDataSplit[0];
    final Dataset<Row> testingData = trainingAndTestDataSplit[1];
    return new MachineLearningData(trainingData, testingData);

  }

  private static void performMachineLearning(MachineLearningData machineLearningData) {
      //@TODO: Left off here
  }

  public static class MachineLearningData {

    public final Dataset<Row> trainingData;
    public final Dataset<Row> testingData;

    public MachineLearningData(Dataset<Row> trainingData,
        Dataset<Row> testingData) {
      this.trainingData = trainingData;
      this.testingData = testingData;
    }
  }
}
