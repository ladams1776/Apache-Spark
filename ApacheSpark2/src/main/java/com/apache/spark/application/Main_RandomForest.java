package com.apache.spark.application;

import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Logger.getLogger;
import static org.apache.spark.sql.functions.col;

import com.apache.spark.domain.shared.LabelMapper;
import com.apache.spark.domain.shared.VariableIndicatorMapper;
import com.apache.spark.domain.shared.OutcomeCorrelationReport;
import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import com.apache.spark.infrastructure.randomforest.CorrelationReport;
import com.apache.spark.infrastructure.randomforest.fullVariableMapper.FullVariableLabelMapper;
import com.apache.spark.infrastructure.randomforest.fullVariableMapper.FullVariableMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Main_RandomForest {

  private final static SparkConnection sparkConnection = new SparkConnectionBuilder().build();

  // @TODO: can uncomment these and comment out the next 2 to get different results.
//  private final static BankVariableIndicatorMapper<Row, StructType> indicatorMapper = new MaritalEducationVariableMapper();
//  private final static BankLabelMapper<Row, LabeledPoint> bankLabelMapper = new MaritalEducationLabelMapper();

  private final static VariableIndicatorMapper<Row, StructType> indicatorMapper = new FullVariableMapper();
  private final static LabelMapper<Row, LabeledPoint> LABEL_MAPPER = new FullVariableLabelMapper();

  private final static OutcomeCorrelationReport<StructType, Dataset<Row>> correlationReport = new CorrelationReport();

  public static void main(String[] args) {
    getLogger("org").setLevel(ERROR);
    getLogger("akka").setLevel(ERROR);
    System.out.println("RandomForest");

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

    // Change DF to RDD. To make indicator variables (map the values over).
    JavaRDD<Row> paritionedBanks = banks.toJavaRDD().repartition(2);

    // There is a bug with Apache spark apparently. I can't do a method reference here ☹️.
    final JavaRDD<Row> indicatedVariables = paritionedBanks.map(row -> indicatorMapper.map(row));

    // Change RDD back to DFR
    final Dataset<Row> cleansedBanks = sparkSession.createDataFrame(indicatedVariables, indicatorMapper.getSchema());
    System.out
        .println("Transformed/Cleansed data - applying indicator variables to the Bank Schema");
    cleansedBanks.show(10);
    cleansedBanks.printSchema();

    // ******************** Analyze Data ************************************* //
    correlationReport.apply(indicatorMapper.getSchema(), cleansedBanks);

    // ******************** Prepare for Machine Learning ********************* //
    // Convert DF to labeled Point Structure. Reuse variable, no need to make another name
    paritionedBanks = cleansedBanks.toJavaRDD().repartition(2);

    final JavaRDD<LabeledPoint> labeledBanks = paritionedBanks.map(row -> LABEL_MAPPER.map(row));

    final Dataset<Row> bankLabels = sparkSession.createDataFrame(labeledBanks, LabeledPoint.class);
    System.out.println("Transformed Label and Features: ");
    bankLabels.show(10);

    // Add an index using string indexer.
    final StringIndexer labelIndex = new StringIndexer()
        .setInputCol("label")
        .setOutputCol("indLabel");

    final StringIndexerModel labelBankModel = labelIndex.fit(bankLabels);
    final Dataset<Row> indexedBankLabel = labelBankModel.transform(bankLabels);
    System.out.println("Indexed Bank LP: ");
    indexedBankLabel.show(10);

    // Perform PCA
    final PCA pca = new PCA()
        .setInputCol("features")
        .setOutputCol("pcaFeatures")
        .setK(3); // 3 is the number of variables you are going to get.

    final PCAModel pcaModel = pca.fit(indexedBankLabel);
    final Dataset<Row> bankPCA = pcaModel.transform(indexedBankLabel);
    System.out.println("PCA'ed Indexed Bank LP: ");
    bankPCA.show(10);

    // Split the data into training and test sets (30% held out for testing).
    final Dataset<Row>[] randomSplit = bankPCA.randomSplit(new double[]{0.7, 0.3});
    final Dataset<Row> trainingData = randomSplit[0];
    final Dataset<Row> testingData = randomSplit[1];

    // ******************** Prepare for Machine Learning ********************* //
    // Create the object
    // Train a DecisionTree model.
    final RandomForestClassifier randomForestClassifier = new RandomForestClassifier()
        .setLabelCol("indLabel")
        .setFeaturesCol("pcaFeatures");

    // Convert indexed labels back to original labels
    final IndexToString labelConverter = new IndexToString()
        .setInputCol("indLabel")
        .setOutputCol("labelStr")
        .setLabels(labelBankModel.labels());

    final IndexToString predictionConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictionStr")
        .setLabels(labelBankModel.labels());

    final RandomForestClassificationModel trainedRFModel = randomForestClassifier.fit(trainingData);

    // Predict on test data
    final Dataset<Row> rawPredictions = trainedRFModel.transform(testingData);

    final Dataset<Row> predictions = predictionConverter
        .transform(labelConverter.transform(rawPredictions));

    // View results
    System.out.println("Result sample :");
    predictions.select("labelStr", "predictionStr", "features").show(10);

    // View confusion matrix
    System.out.println("Confusion Matrix : ");
    predictions.groupBy(col("labelStr"), col("predictionStr")).count().show();

    // Accuracy computation
    final double accuracy = new MulticlassClassificationEvaluator()
        .setLabelCol("indLabel")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
        .evaluate(predictions);

    System.out.println("Accuracy = " + Math.round(accuracy * 100) + "%");

  }

}
