package com.fl.spark.core.java.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author fl37804
 * @date 2022-07-21
 * <p>
 * 创建rdd的方式有多种
 * 从文件读取、从数据源获取、手动创建
 * 步骤都是：
 * 1、创建sparkconf进行配置
 * 2、创建JavaSparkContext
 * 3、创建JavaRDD
 * 注意：SparkSession是用在SparkSQL、SparkStreaming上
 */

public class CreateRDD {
    public static void main(String[] args) {
        // 1.手动创建
        SparkConf conf = new SparkConf();
        conf.setAppName("rdd dev");
        conf.setMaster("local");

        // local 设置的是单线程启动  local[*] 是根据服务器来决定，如果是8核，那就启用8个线程
        JavaSparkContext sc = new JavaSparkContext("local", "first test", conf);

        List<String> list = new ArrayList<>();
        list.add("张三");
        list.add("李四");
        list.add("王五");
        list.add("王五");
        JavaRDD<String> javaRDD = sc.parallelize(list);

//        System.out.println(javaRDD);

        // 2.从文件中读取

        JavaRDD<String> stringJavaRDD = sc.textFile("src/main/resources/data/fl.txt");
        System.out.println(stringJavaRDD);


    }
}
