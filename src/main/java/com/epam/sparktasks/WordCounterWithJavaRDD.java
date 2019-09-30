package com.epam.sparktasks;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCounterWithJavaRDD {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\winutils");
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .set("spark.hadoop.validateOutputSpecs", "false")
                .setAppName("OrderBy");
        String path = "/csv/*.csv";

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd = sc.textFile("cogsley_sales.csv");
        JavaPairRDD<String, Integer> counts =
                rdd.flatMap(x -> Arrays.asList(x.split(",")).iterator())
                        .mapToPair(x -> new Tuple2<>(x, 1))
                        .reduceByKey((x, y) -> x + y);

        counts.repartition(4).saveAsTextFile("res_combined1");
        sc.close();

    }
}
