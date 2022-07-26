package com.fl.spark.core.java.api;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author fl37804
 * @date 2022-07-22
 * <p>
 * spark 常用算子
 */
public class OperatorDev {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("常用算子demo");
        //  创建JavaSparkContext对象
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

//        testMap(sc);

//        testFilter(sc);

//        testFlatMap(sc);

//        testGroupByKey(sc);

//        testReduceByKey(sc);

//        sortByKey(sc);

//        join(sc);

        cogroup(sc);
    }

    /**
     * Map 算子的使用
     *
     * @param sc
     */
    private static void testMap(JavaSparkContext sc) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> parallelizeRdd = sc.parallelize(list);

        /**
         * 使用map算子;将集合中的每个元素都乘以2
         * map算子，是对任何类型的RDD，都可以调用
         * 在java中map算子接收的参数是Function对象
         * 创建的Function对象，一定会让你设置第二个泛型参数，这个泛型类型，就是返回新元素的类型
         * 同时call()方法的返回类型，也必须与第二个泛型类型同步。
         * 在call方法内部，就可以对原始RDD中的每个元素进行各种处理和计算，并返回一个新的元素。
         * 所有新的元素就会组成一个新的RDD。
         */
        JavaRDD<Integer> mapRdd = parallelizeRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });

        // 数据打印
        mapRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * Filter 过滤数数字是偶数的
     *
     * @param sc
     */
    private static void testFilter(JavaSparkContext sc) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> parallelizeRdd = sc.parallelize(list);

        JavaRDD<Integer> filterRdd = parallelizeRdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer num) throws Exception {
                return num % 2 == 0;
            }
        });

        filterRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * FlateMap 处理数据
     *
     * @param sc
     */
    private static void testFlatMap(JavaSparkContext sc) {
        List<String> list = Arrays.asList("hello you", "hello me", "hello world");

        JavaRDD<String> stringJavaRDD = sc.parallelize(list);

        JavaRDD<String> javaRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });

        javaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    /**
     * groupByKey：按照班级将成绩分组
     * 使用groupByKey后返回的算子类型依然是JavaPairRDD 但是第二个参数泛型是Iterable<Integer>
     */
    private static void testGroupByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(
                new Tuple2<String, Integer>("英语", 150),
                new Tuple2<String, Integer>("理综", 290),
                new Tuple2<String, Integer>("语文", 140),
                new Tuple2<String, Integer>("语文", 142),
                new Tuple2<String, Integer>("英语", 149),
                new Tuple2<String, Integer>("数学", 145));

        // 并行化集合，创建JavaPairRDD 每个元素是tuple2
        JavaPairRDD<String, Integer> javaPairRDD = sc.parallelizePairs(tuple2List);
        //  根据key 分组
        JavaPairRDD<String, Iterable<Integer>> groupByKey = javaPairRDD.groupByKey();

        groupByKey.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                System.out.println(stringIterableTuple2);
            }
        });
    }


    private static void testReduceByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scoresList = Arrays.asList(new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 87),
                new Tuple2<String, Integer>("class2", 90),
                new Tuple2<String, Integer>("class1", 65));

        //并行化集合，创建JavaPairRDD 每个元素是tuple2
        JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoresList);
        /**
         * reduceByKey接收的参数是Function2类型，它有3个泛型参数，前两个泛型参数类型代表原始的RDD value的类型
         * 因此 ,对每个key 进行reduce 都会依次将第一个、第二个value传入 然后将得到的值载与第三个value传入
         * 因此此处, 会自动的定义两个泛型类型 代表call()方法的两个传入的参数的类型
         * 第三个泛型类型，代表了每次reduce操作返回的值类型，默认也是与原始的RDD的value类型相同的。
         * reduceByKey算的返回的RDD 还是JavaPairRDD<Key,value>
         */

        JavaPairRDD<String, Integer> reduceRDD = scores.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t1) throws Exception {
                System.out.println("class:" + t1._1 + " " + "totalscore:" + t1._2);
            }
        });

    }


    /**
     * sortByKey:根据分数进行排序
     *
     * @param sc
     */
    public static void sortByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, String>> scoresList =
                Arrays.asList(
                        new Tuple2<Integer, String>(65, "leo"),
                        new Tuple2<Integer, String>(50, "leo1"),
                        new Tuple2<Integer, String>(100, "leo2")

                );

        JavaPairRDD<Integer, String> scoreRDD = sc.parallelizePairs(scoresList, 1);

        /**
         * Java中的sortByKey方法中接收的参数是 boolean 默认是true 升序排序
         */
        JavaPairRDD<Integer, String> retRDD = scoreRDD.sortByKey();
        retRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t1) throws Exception {
                System.out.println(t1._1 + ":" + t1._2);
            }
        });

    }

    /**
     * join:打印每个学生的成绩
     * <p>
     * join  这个 就是 key相同的join，然后取 join这两个key 的 value的并集 作为value
     *
     * @param sc
     */
    public static void join(JavaSparkContext sc) {

        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "marry"),
                new Tuple2<>(4, "ff"));
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<>(1, 80),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 100),
                new Tuple2<>(4, 60));
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, Tuple2<String, Integer>> retRDD = studentRDD.join(scoreRDD);
        /**
         *使用 join算子 关联两个RDD 是等值连接
         * join以后 是根据key进行join的，并且返回JavapairRDD
         * JavaPairRDD的第一个泛型是之前两个RDD的key类型 第二个泛型类型是Tuple2<v1,v2>类型
         * Tuple2的两个泛型分别是原始RDD的value类型
         *  比如 （1,1） （1,2） （1,3）的RDD
         * 还有一个 （1,4） （2,1） （2,2）
         *  join以后 实际上得到 （1，（1,4）） （1（2,4）） （1，,3,4））
         */
        retRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t1) throws Exception {
                System.out.println(t1);
            }
        });
    }


    /**
     * cogroup:打印每个学生的成绩
     *
     * @param sc
     */
    public static void cogroup(JavaSparkContext sc) {

        List<Tuple2<Integer, String>> students = Arrays.asList(
                new Tuple2<>(1, "tom"),
                new Tuple2<>(2, "jack"),
                new Tuple2<>(3, "marry"),
                new Tuple2<>(4, "ff"));
        List<Tuple2<Integer, Integer>> scores = Arrays.asList(
                new Tuple2<>(1, 80),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 100),
                new Tuple2<>(1, 60),
                new Tuple2<>(2, 80));
        JavaPairRDD<Integer, String> studentRDD = sc.parallelizePairs(students);
        JavaPairRDD<Integer, Integer> scoreRDD = sc.parallelizePairs(scores);
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> retRDD = studentRDD.cogroup(scoreRDD);
        /**
         *cogroup和join不同
         * 相当于 一个 key join上所有的value 都放到一个Iterable里面去了
         * cogroup不太好讲解 须有动手编写案例仔细体会。
         */
        retRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t1) throws Exception {
                System.out.println(t1);

            }
        });
    }


}
