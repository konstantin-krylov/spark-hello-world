package com.epam.linkedintasks;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Example1 {
    private static final String PATH = "state_info.csv";
    private static SparkSession sparkSession = getSparkSession();

    private static SparkSession getSparkSession() {
        System.setProperty("hadoop.home.dir", "C:\\Users\\Konstantin_Krylov2\\IdeaProjects\\spark-hello-world\\src\\main\\resources");
        SparkSession.Builder config = SparkSession.builder()
                .appName("Hello-world")
                .config("spark.sql.orc.impl", "native")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.kryoserializer.buffer.max", "128m")
                .config("spark.kryoserializer.buffer", "64m")
                .config("spark.sql.codegen.aggregate.map.twolevel.enabled", "true")
                .config("spark.sql.shuffle.partitions", 100)
                .master("local[*]");
        return config.getOrCreate();
    }

    public static void main(String[] args) {
        showSchema();

        showStates();

        JavaSparkContext sc = getJavaSparkContext();

        printCountDebug(sc);

        printCountFiles(sc);

    }

    private static void printCountFiles(JavaSparkContext sc) {
        String paths = "C:\\Users\\Konstantin_Krylov2\\IdeaProjects\\spark-hello-world\\sales_log\\*csv";
        JavaPairRDD<String, String> files = sc.wholeTextFiles(paths);
        System.out.println(files.count());
    }

    private static void printCountDebug(JavaSparkContext sc) {
        String path = "studstat-debug.2019-09-18.0.log";

        JavaRDD<String> rdd = sc.textFile(path).cache();
        long count = rdd
                .filter(x -> x.contains("DEBUG"))
                .count();
        System.out.println(count);
    }

    private static JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local[*]")
                .setAppName("WorkingWithTextFile");

        return new JavaSparkContext(conf);
    }


    private static void showStates() {
        Dataset<Row> ds = getRowDataset();
        Dataset<Row> lastSale = ds.select("State").distinct().orderBy("State");
        lastSale.show();
    }

    private static Dataset<Row> getRowDataset() {
        return sparkSession.read().format("csv").option("inferSchema", "true")
                .option("header", "true").load(PATH);
    }

    private static void showSchema() {
        Dataset<Row> ds = getRowDataset();
        ds.printSchema();
    }
}
