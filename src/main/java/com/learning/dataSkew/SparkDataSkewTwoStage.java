package com.learning.dataSkew;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SparkDataSkewTwoStage {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        int slices = 3;
        List<String> list1 = new ArrayList(Arrays.asList("hello 1",
                "hello 2",
                "hello 3",
                "hello 4",
                "you 1",
                "me 2"));

        JavaPairRDD<String, Long> rdd = jsc.parallelize(list1, slices).mapToPair(
                new PairFunction<String, String, Long>() {

                    @Override
                    public Tuple2<String, Long> call(String s) throws Exception {
                        String[] arr = s.split(" ");

                        return new Tuple2<String, Long>(arr[0], Long.parseLong(arr[1]));
                    }
                }
        );
        // 第一步，给RDD中的每个key都打上一个随机前缀。
        JavaPairRDD<String, Long> randomPrefixRdd = rdd.mapToPair(
                new PairFunction<Tuple2<String, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple) throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(10);
                        return new Tuple2<String, Long>(prefix + "_" + tuple._1, tuple._2);
                    }
                }
        );

        // 第二步，对打上随机前缀的key进行局部聚合。
        JavaPairRDD<String, Long> localAggrdd = randomPrefixRdd.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

// 第三步，去除RDD中每个key的随机前缀。
        JavaPairRDD<String, Long> removedandomPrefixdd = localAggrdd.mapToPair(
                new PairFunction<Tuple2<String, Long>, String, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple)
                            throws Exception {
                        String originalKey = tuple._1.split("_")[1];
                        return new Tuple2<String, Long>(originalKey, tuple._2);
                    }
                });

// 第四步，对去除了随机前缀的RDD进行全局聚合。
        JavaPairRDD<String, Long> globalAggrdd = removedandomPrefixdd.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        List<Tuple2<String, Long>> output = globalAggrdd.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        spark.stop();

    }
}
