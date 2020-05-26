package com.learning.dataSkew;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkJoinDataSkew {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        SparkContext sc = spark.sparkContext();

        int slices = 3;
        List<String> list2 = new ArrayList(Arrays.asList(
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "1  郑祥楷1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "2  王佳豪1",
                "3  刘鹰2",
                "4  宋志华3",
                "5  刘帆4",
                "6  OLDLi5"
        ));

        List<String> list1 = new ArrayList(Arrays.asList(
                "1 1807bd-bj",
                "2 1807bd-sz",
                "3 1807bd-wh",
                "4 1807bd-xa",
                "7 1805bd-bj"
        ));

        JavaPairRDD<Long, String> rdd1 = jsc.parallelize(list1, slices).mapToPair(
                new PairFunction<String, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {
                        String[] arr = s.split(" ");

                        return new Tuple2<Long, String>(Long.parseLong(arr[0]), arr[1]);
                    }
                }
        );

        JavaPairRDD<Long, String> rdd2 = jsc.parallelize(list2, slices).mapToPair(
                new PairFunction<String, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {
                        String[] arr = s.split("  ");

                        return new Tuple2<Long, String>(Long.parseLong(arr[0]), arr[1]);
                    }
                }
        );

        // 首先将其中一个key分布相对较为均匀的RDD膨胀100倍。
        JavaPairRDD<String, String> expandedRDD = rdd1.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, String>, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        for (int i = 0; i < 100; i++) {
                            list.add(new Tuple2<String, String>(i + "_" + tuple._1, tuple._2));
                        }
                        return list.iterator();
                    }
                });

        List list11 = expandedRDD.collect();


        // 其次，将另一个有数据倾斜key的RDD，每条数据都打上100以内的随机前缀。
        JavaPairRDD<String, String> mappedRDD = rdd2.mapToPair(
                new PairFunction<Tuple2<Long, String>, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(100);
                        return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
                    }
                });

        List list12 = mappedRDD.collect();


        // 将两个处理后的RDD进行join即可。
        JavaPairRDD<String, Tuple2<String, String>> joinedRDD = mappedRDD.join(expandedRDD);

        JavaPairRDD<String, Tuple2<String, String>> returltRDD = joinedRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, String>>, String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
                        String[] arr = tuple._1.split("_");
                        return new Tuple2<String, Tuple2<String, String>>(arr[1], tuple._2);
                    }
                }
        );


        List<Tuple2<String, Tuple2<String, String>>> output = returltRDD.collect();
        for (Tuple2<?, ?> tuple : output) {
            Tuple2<String, String> tuple2 = (Tuple2<String, String>) tuple._2();
            System.out.println(tuple._1() + ": " + tuple2._1 + ": " + tuple2._2());
        }
        spark.stop();

    }
}
