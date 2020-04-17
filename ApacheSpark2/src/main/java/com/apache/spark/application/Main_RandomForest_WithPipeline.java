package com.apache.spark.application;

import com.apache.spark.domain.shared.LabelMapper;
import com.apache.spark.domain.shared.VariableIndicatorMapper;
import com.apache.spark.domain.shared.CleanseData;
import com.apache.spark.infrastructure.randomforest.CleanseBanks;
import com.apache.spark.domain.shared.OutcomeCorrelationReport;
import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import com.apache.spark.infrastructure.randomforest.CorrelationReport;
import com.apache.spark.infrastructure.randomforest.fullVariableMapper.FullVariableLabelMapper;
import com.apache.spark.infrastructure.randomforest.fullVariableMapper.FullVariableMapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.log4j.Level.ERROR;
import static org.apache.log4j.Logger.getLogger;
import static org.apache.spark.sql.functions.col;

public class Main_RandomForest_WithPipeline {

    private final static SparkConnection sparkConnection = new SparkConnectionBuilder().build();
    private final static SparkSession sparkSession = sparkConnection.getSparkSession();
    private final static CleanseData<Dataset<Row>> cleanseBanks = new CleanseBanks(sparkSession);
    private final static VariableIndicatorMapper<Row, StructType> indicatorMapper = new FullVariableMapper();
    private final static LabelMapper<Row, LabeledPoint> LABEL_MAPPER = new FullVariableLabelMapper();
//  private final static BankLabelMapper<Row, LabeledPoint> bankLabelMapper = new MaritalEducationLabelMapper();

    private final static OutcomeCorrelationReport<StructType, Dataset<Row>> correlationReport = new CorrelationReport();

    public static void main(String[] args) {
        getLogger("org").setLevel(ERROR);
        getLogger("akka").setLevel(ERROR);
        System.out.println("RandomForest");

        // ******************** Setup ************************************* //

        final Dataset<Row> banks = sparkSession
                .read()
                .option("header", "true")
                .option("sep", ";")
                .csv("src/main/resources/bank.csv");

        banks.show(5);
        banks.printSchema();

        // ******************** Cleanse Data ************************************* //
        final Dataset<Row> cleansedBanks = cleanseBanks.apply(banks);

        // ******************** Analyze Data ************************************* //
        correlationReport.apply(indicatorMapper.getSchema(), cleansedBanks);

        // ******************** Prepare for Machine Learning ********************* //
        // Convert DF to labeled Point Structure. Reuse variable, no need to make another name
        final JavaRDD<Row> paritionedBanks = cleansedBanks.toJavaRDD().repartition(2);

        final JavaRDD<LabeledPoint> labeledBanks = paritionedBanks.map(row -> LABEL_MAPPER.map(row));
        final Dataset<Row> bankLabels = sparkSession.createDataFrame(labeledBanks, LabeledPoint.class);

        // Split the data into training and test sets (30% held out for testing).
        final Dataset<Row>[] randomSplit = sparkSession
                .createDataFrame(labeledBanks, LabeledPoint.class)
                .randomSplit(new double[]{0.7, 0.3});

        final Dataset<Row> trainingData = randomSplit[0];
        final Dataset<Row> testingData = randomSplit[1];

        // Add an index using string indexer.
        final StringIndexer labelIndex = new StringIndexer()
                .setInputCol("label")
                .setOutputCol("indLabel");

        final StringIndexerModel labelBankModel = labelIndex.fit(bankLabels);

        // Perform PCA
        final PCA pca = new PCA()
                .setInputCol("features")
                .setOutputCol("pcaFeatures")
                .setK(3); // 3 is the number of variables you are going to get.

        // Convert indexed labels back to original labels
        final IndexToString labelConverter = new IndexToString()
                .setInputCol("indLabel")
                .setOutputCol("labelStr")
                .setLabels(labelBankModel.labels());

        final IndexToString predictionConverter = new IndexToString()
                .setInputCol("prediction")
                .setOutputCol("predictionStr")
                .setLabels(labelBankModel.labels());

        // Train a DecisionTree model.
        final RandomForestClassifier randomForestClassifier = new RandomForestClassifier()
                .setLabelCol("indLabel")
                .setFeaturesCol("pcaFeatures");

        final Dataset<Row> predictions = new Pipeline()
                .setStages(new PipelineStage[]{
                        labelBankModel,
                        pca,
                        randomForestClassifier,
                        predictionConverter,
                        labelConverter,
                })
                .fit(trainingData)
                .transform(testingData);

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
