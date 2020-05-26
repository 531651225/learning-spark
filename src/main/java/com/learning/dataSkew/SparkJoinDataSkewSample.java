package com.learning.dataSkew;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class SparkJoinDataSkewSample {
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



        // 首先从包含了少数几个导致数据倾斜key的rdd1中，采样10%的样本数据。
        JavaPairRDD<Long, String> sampledRDD = rdd1.sample(false, 0.1);


        // 对样本数据RDD统计出每个key的出现次数，并按出现次数降序排序。
        // 对降序排序后的数据，取出top 1或者top 100的数据，也就是key最多的前n个数据。
        // 具体取出多少个数据量最多的key，由大家自己决定，我们这里就取1个作为示范。
        JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
                new PairFunction<Tuple2<Long,String>, Long, Long>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        return new Tuple2<Long, Long>(tuple._1, 1L);
                    }
                });
        JavaPairRDD<Long, Long> countedSampledRDD = mappedSampledRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        JavaPairRDD<Long, Long> reversedSampledRDD = countedSampledRDD.mapToPair(
                new PairFunction<Tuple2<Long,Long>, Long, Long>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple)
                            throws Exception {
                        return new Tuple2<Long, Long>(tuple._2, tuple._1);
                    }
                });

        final Long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;


// 从rdd1中分拆出导致数据倾斜的key，形成独立的RDD。
        JavaPairRDD<Long, String> skewedRDD = rdd1.filter(
                new Function<Tuple2<Long,String>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<Long, String> tuple) throws Exception {
                        return tuple._1.equals(skewedUserid);
                    }
                });
// 从rdd1中分拆出不导致数据倾斜的普通key，形成独立的RDD。
        JavaPairRDD<Long, String> commonRDD = rdd1.filter(
                new Function<Tuple2<Long,String>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<Long, String> tuple) throws Exception {
                        return !tuple._1.equals(skewedUserid);
                    }
                });


        // rdd2，就是那个所有key的分布相对较为均匀的rdd。
// 这里将rdd2中，前面获取到的key对应的数据，过滤出来，分拆成单独的rdd，并对rdd中的数据使用flatMap算子都扩容100倍。
// 对扩容的每条数据，都打上0～100的前缀。
        JavaPairRDD<String, String> skewedUserid2infoRDD = rdd2.filter(
                new Function<Tuple2<Long,String>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Boolean call(Tuple2<Long, String> tuple) throws Exception {
                        return tuple._1.equals(skewedUserid);
                    }
                }).
                flatMapToPair(new PairFlatMapFunction<Tuple2<Long,String>, String, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Iterator<Tuple2<String, String>> call(
                    Tuple2<Long, String> tuple) throws Exception {
                Random random = new Random();
                List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                for(int i = 0; i < 100; i++) {
                    list.add(new Tuple2<String, String>(i + "_" + tuple._1, tuple._2));
                }
                return list.iterator();
            }

        });

// 将rdd1中分拆出来的导致倾斜的key的独立rdd，每条数据都打上100以内的随机前缀。
// 然后将这个rdd1中分拆出来的独立rdd，与上面rdd2中分拆出来的独立rdd，进行join。
        JavaPairRDD<Long, Tuple2<String, String>> joinedRDD1 = skewedRDD.mapToPair(
                new PairFunction<Tuple2<Long,String>, String, String>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(100);
                        return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
                    }
                })
                .join(skewedUserid2infoRDD)
                .mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>, Long, Tuple2<String, String>>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<Long, Tuple2<String, String>> call(
                            Tuple2<String, Tuple2<String, String>> tuple)
                            throws Exception {
                        long key = Long.valueOf(tuple._1.split("_")[1]);
                        return new Tuple2<Long, Tuple2<String, String>>(key, tuple._2);
                    }
                });

// 将rdd1中分拆出来的包含普通key的独立rdd，直接与rdd2进行join。
        JavaPairRDD<Long, Tuple2<String, String>> joinedRDD2 = commonRDD.join(rdd2);

// 将倾斜key join后的结果与普通key join后的结果，uinon起来。
// 就是最终的join结果。
        JavaPairRDD<Long, Tuple2<String, String>> joinedRDD = joinedRDD1.union(joinedRDD2);
        List<Tuple2<Long, Tuple2<String, String>>> output = joinedRDD.collect();
        for (Tuple2<?, ?> tuple : output) {
            Tuple2<String, String> tuple2 = (Tuple2<String, String>)tuple._2();
            System.out.println(tuple._1() + ": " + tuple2._1+ ": " +tuple2._2());
        }
        spark.stop();

    }
}
