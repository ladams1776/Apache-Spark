package com.apache.spark.application;

import static org.apache.spark.sql.functions.col;

import com.apache.spark.infrastructure.SparkConnection;
import com.apache.spark.infrastructure.SparkConnection.SparkConnectionBuilder;
import com.apache.spark.infrastructure.reports.filters.EverythingButStringTypesFilter;
import java.util.Arrays;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Main_DecisionTree {

  private static final SparkConnection sparkConnection = new SparkConnectionBuilder().build();
  private static final SparkSession sparkSession = sparkConnection.getSparkSession();
  private static final JavaSparkContext sparkContext = sparkConnection.getSpContext();

  public static void main(String[] args) {
    System.out.println("Working");

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
    analyzeData(cleanedIris, irisSchema);


  }

  private static Dataset<Row> cleanseData(Dataset<Row> iris, StructType irisSchema) {

    final JavaRDD<Row> irisRepartitioned = iris.toJavaRDD().repartition(2);

    final JavaRDD<Row> irisValuesCasted = irisRepartitioned.map(row ->
        RowFactory.create(Double.valueOf(row.getString(0)),
            Double.valueOf(row.getString(1)),
            Double.valueOf(row.getString(2)),
            Double.valueOf(row.getString(3)),
            row.getString(4)));

    final Dataset<Row> irisCleansed = sparkSession.createDataFrame(irisValuesCasted, irisSchema);

    System.out.println("Cleaned Data (aka Transformed)");
    iris.show(5);

    return irisCleansed;
  }

  private static void analyzeData(Dataset<Row> cleanedIris, StructType irisSchema) {
    //@TODO: Need a refresher on this one. I can't remember what we are doing here.
    // If I was to try to guess/recall, it would be... that we are... Declaring that the field,
    // named 'SPECIES', has an indexed alias name that can be referenced - 'IND_SPECIES'.
    final StringIndexer indexer = new StringIndexer()
        .setInputCol("SPECIES")
        .setOutputCol("IND_SPECIES");

    final StringIndexerModel siModel = indexer.fit(cleanedIris);
    final Dataset<Row> indexedIris = siModel.transform(cleanedIris);
    indexedIris.groupBy(col("SPECIES"), col("IND_SPECIES")).count().show();

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
  }

}
