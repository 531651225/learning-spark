package com.learning.dataSkew;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkDataBroadcast {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
                .master("local[*]")
                .getOrCreate();

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        SparkContext sc = spark.sparkContext();

        int slices = 3;
        List<String> list1 = new ArrayList(Arrays.asList(
                "1  郑祥楷1",
                "2  王佳豪1",
                "3  刘鹰2",
                "4  宋志华3",
                "5  刘帆4",
                "6  OLDLi5"
        ));

        List<String> list2 = new ArrayList(Arrays.asList(
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
                        String[] arr = s.split("  ");

                        return new Tuple2<Long, String>(Long.parseLong(arr[0]), arr[1]);
                    }
                }
        );

        JavaPairRDD<Long, String> rdd2 = jsc.parallelize(list2, slices).mapToPair(
                new PairFunction<String, Long, String>() {

                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {
                        String[] arr = s.split(" ");

                        return new Tuple2<Long, String>(Long.parseLong(arr[0]), arr[1]);
                    }
                }
        );
        // 首先将数据量比较小的RDD的数据，collect到Driver中来。
        List<Tuple2<Long, String>> rdd1Data = rdd1.collect();
        final Broadcast<List<Tuple2<Long, String>>> rdd1DataBroadcast = jsc.broadcast(rdd1Data);
        // 然后使用Spark的广播功能，将小RDD的数据转换成广播变量，这样每个Executor就只有一份RDD的数据。
        // 可以尽可能节省内存空间，并且减少网络传输性能开销。
        // 对另外一个RDD执行map类操作，而不再是join类操作。
        JavaPairRDD<Long, Tuple2<String, String>> joinedRdd = rdd2.mapToPair(
                new PairFunction<Tuple2<Long, String>, Long, Tuple2<String, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        // 在算子函数中，通过广播变量，获取到本地Executor中的rdd1数据。
                        List<Tuple2<Long, String>> rdd1Data = rdd1DataBroadcast.value();
                        // 可以将rdd1的数据转换为一个Map，便于后面进行join操作。
                        Map<Long, String> rdd1DataMap = new HashMap<Long, String>();
                        for (Tuple2<Long, String> data : rdd1Data) {
                            rdd1DataMap.put(data._1, data._2);
                        }
                        // 获取当前RDD数据的key以及value。
                        Long key = tuple._1;
                        String value = tuple._2;
                        // 从rdd1数据Map中，根据key获取到可以join到的数据。
                        String rdd1Value = rdd1DataMap.get(key);
                        return new Tuple2<Long, Tuple2<String, String>>(key, new Tuple2<String, String>(value, rdd1Value));
                    }
                });

        List<Tuple2<Long, Tuple2<String, String>>> output = joinedRdd.collect();
        for (Tuple2<?, ?> tuple : output) {
            Tuple2<String, String> tuple2 = (Tuple2<String, String>)tuple._2();
            System.out.println(tuple._1() + ": " + tuple2._1+ ": " +tuple2._2());
        }
        spark.stop();

    }
}
