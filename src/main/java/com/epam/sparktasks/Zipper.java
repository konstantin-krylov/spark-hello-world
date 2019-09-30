package com.epam.sparktasks;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;

import static org.apache.spark.sql.functions.col;

public class Zipper {
    private static final String PATH = "data.csv";
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

    private static void getSquareOfRowWithRDdd() {
        List<Integer> collect = sparkSession.read().csv("data.csv")
                .select(
                        col("_c0").alias("number1").cast(DataTypes.IntegerType),
                        col("_c1").alias("number2").cast(DataTypes.IntegerType),
                        col("_c2").alias("number3").cast(DataTypes.IntegerType)
                )
                .javaRDD()
                .map(row -> {
                    Integer res = row.getInt(0) + row.getInt(1) + row.getInt(2);
                    return res;
                })
                .coalesce(1)
                .map(x -> x * x)
                .collect();
        System.out.println(collect);
    }

    private static void printSchema() {
        sparkSession.read().textFile(PATH).select(
                col("_c0").alias("number1").cast(DataTypes.IntegerType),
                col("_c1").alias("number2").cast(DataTypes.IntegerType),
                col("_c2").alias("number3").cast(DataTypes.IntegerType)
        )
                .printSchema();
    }

    private static void showRows() {
        //setting the table schema and passing it to the session
        StructField[] structFields = {
                DataTypes.createStructField("number1", DataTypes.IntegerType, true),
                DataTypes.createStructField("number2", DataTypes.IntegerType, true),
                DataTypes.createStructField("number3", DataTypes.IntegerType, true),
        };
        sparkSession.read()
                .schema(new StructType(structFields))
                .csv(PATH)
                .show();
    }

    private static void sumOfRowsAndOrderByCSV() {
        sparkSession.read().csv("data.csv")
                .select(
                        col("_c0").alias("number1").cast(DataTypes.IntegerType),
                        col("_c1").alias("number2").cast(DataTypes.IntegerType),
                        col("_c2").alias("number3").cast(DataTypes.IntegerType)
                )
//                .map(value -> value.getInt(0) + value.getInt(1) + value.getInt(2), Encoders.INT())
                .withColumn("res", col("number1").plus(col("number2").plus(col("number3"))))
                .orderBy(col("number1").desc())
                .coalesce(1)
                .write()
                .option("header", true)
                .mode(SaveMode.Overwrite)
                .csv("res_combined");
    }

    public static void main(String[] args) {
        sumOfRowsAndOrderByCSV();
        showRows();
        printSchema();
        getSquareOfRowWithRDdd();
    }
}

