package com.fl.spark.core.java.rdd;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Iterator;

/**
 * @author fl37804
 * @date 2022-07-21
 */
public class StudyRDD {
    public static void main(String[] args) {
        //  编写spark应用程序
        //  第一步：创建sparkconf对象，设置spark应用配置信息
        //  使用setMaster()设置Spark应用程序要连接的Spark集群的Master节点的url
        //  如果设置local则代表在本地执行
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local");
        //  第二步: 创建JavaSparkContext对象
        //  在Spark中SparkContext是spark所有功能的入口，无论使用java、scala还是python编写都必须有一个sparkContext对象.
        //  它的作用：初始化spark应用程序所需要的的一些核心组件，包括调度器（DAGSchedule、TaskSchedule）
        //  还回去spark Master节点上进行注册等等。
        //  但是呢，在spark中编写不同类型的spark应用程序使用的spakContext是不同的，
        //  如果使用scala那么就是sparkContext对象
        //  如果使用java那么就是JavaSparkContext对象
        //  如果开发spark Sql程序那么就是SQLContext或者HiveContext
        //  如果是开发Spark Streaming程序，那么就是它独有的SparkContext。
        JavaSparkContext sc = new JavaSparkContext(conf);
        //  第三步：从数据源(HDFS、本地文件等)加载数据 创建一个RDD
        //  parkContext中根据文件的类型输入源创建的RDD的方法 是textFile()
        //  在java中，创建的普通RDD都叫做JavaRDD
        //  在这里呢,RDD有元素的概念，如果是HDFS或者是本地文件创建的RDD，其每个元素相当于文件的一行数据。
        JavaRDD<String> linesRDD = sc.textFile("F:\\sprak-wrok\\sprak-dev\\spark-core\\src\\main\\resources\\data\\fl.txt");
        //  第四步: 对初始的RDD进行transformation操作
        //  将每一行拆分成单个的单词
        JavaRDD<String> words = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {

                return new ArrayIterator(s.split(","));
            }
        });
        JavaPairRDD<String, Integer> word = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> result = word.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer int1, Integer int2) throws Exception {
                return int1 + int2;
            }
        });

        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });

    }
}
