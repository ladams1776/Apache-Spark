package com.apache.spark.application;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.col;

public class Main_NaiveBayes {
    static SparkConnection sparkConnection = new SparkConnectionBuilder().build();
    static SparkSession sparkSession = sparkConnection.getSparkSession();

    public static void main(String[] arg) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        Logger.getLogger("akka").setLevel(Level.ERROR);
        System.out.println("NaiveBayes");


        // ******************** Load Data ************************************* //
        final StructType smsSchema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("label", DataTypes.DoubleType, false),
                DataTypes.createStructField("message", DataTypes.StringType, false)
        });

        final Dataset<Row> smsDF = sparkSession.read()
                .csv("src/main/resources/SMSSpamCollection.csv");

        System.out.println("Original Dataset");
        smsDF.show(10);

        System.out.println("Original Schema");
        smsDF.printSchema();

        // ******************** Cleanse Data ************************************* //
        final JavaRDD<Row> rdd1 = smsDF.toJavaRDD().repartition(2);

        final JavaRDD<Row> mappedRdd = rdd1.map(row -> {
            final double spam = row.getString(0).equals("spam") ? 1.0 : 0.0;
            return RowFactory.create(spam, row.getString(1));
        });

        final Dataset<Row> smsCleansedDf = sparkSession.createDataFrame(mappedRdd, smsSchema);
        System.out.println("Transformed Data: ");
        smsCleansedDf.show(10); // 'label' should be be converted from 'ham' and 'spam' to 0.0 and 1.0

        // ******************** Prepare for Machine Learning ********************* //
        final Dataset<Row>[] randomSplitData = smsCleansedDf.randomSplit(new double[]{.7, .3});
        final Dataset<Row> training = randomSplitData[0];
        final Dataset<Row> testingData = randomSplitData[1];

        // split the message column words up by white space and stuff the values into a column known as "words"
        final Tokenizer tokenizer = new Tokenizer()
                .setInputCol("message")
                .setOutputCol("words");

        // Map a sequence of terms to the term frequencies.
        final HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures");

        //@TODO: Rewatch the video from here forward.
        final IDF idf = new IDF()
                .setInputCol("rawFeatures")
                .setOutputCol("features");

        final NaiveBayes nbClassifier = new NaiveBayes()
                .setLabelCol("label")
                .setFeaturesCol("features");

        // set all the stages to the Pipeline
        final Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer,
                        hashingTF,
                        idf,
                        nbClassifier});

        // Fit the training data on the Pipeline.
        final PipelineModel pipelineModel = pipeline.fit(training);
        final Dataset<Row> predictions = pipelineModel.transform(testingData);

        // View results
        System.out.println("Result sample : ");
        predictions.show(10);

        // View confusion matrix
        System.out.println("Confusion matrix : ");
        predictions.groupBy(col("label"), col("prediction")).count().show(10);

        // Accuracy computation
        final MulticlassClassificationEvaluator classificationEvalutator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        final double accuracy = classificationEvalutator.evaluate(predictions);
        System.out.println("Accuracy = " + Math.round(accuracy * 100) + "%");

    }

}
