package com.apache.spark.stuff;

import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class MainRightOuterJoin {
    public static void main(String[] args) {
        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.49576);
        inputData.add(90.32);
        inputData.add(35223.532);
        inputData.add(3533.5323);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        // setMaster = we are using spark in a local config, we don't have a cluster (* = use all the cores to process this)
        SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
        JavaSparkContext context = new JavaSparkContext(conf);

        List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
        visitsRaw.add(new Tuple2<>(1, 10));
        visitsRaw.add(new Tuple2<>(2, 11));
        visitsRaw.add(new Tuple2<>(3, 12));

        List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
        usersRaw.add(new Tuple2<>(1, "John"));
        usersRaw.add(new Tuple2<>(2, "Bob"));
        usersRaw.add(new Tuple2<>(3, "Alan"));
        usersRaw.add(new Tuple2<>(4, "Doris"));
        usersRaw.add(new Tuple2<>(5, "Marybelle"));
        usersRaw.add(new Tuple2<>(6, "Raquel"));

        JavaPairRDD<Integer, Integer> visits = context.parallelizePairs(visitsRaw);
        JavaPairRDD<Integer, String> users = context.parallelizePairs(usersRaw);

        // outer join
        visits
                .rightOuterJoin(users)
                .foreach(it -> System.out.println(" user " + it._2._2 + " had " + it._2._1.orElse(0)));

        context.close();
    }
}
