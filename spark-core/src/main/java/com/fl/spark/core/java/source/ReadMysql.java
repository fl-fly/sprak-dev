package com.fl.spark.core.java.source;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * @author fl37804
 * @date 2022-06-07
 */
public class ReadMysql {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("read mysql data");
        // 创建一个spark session 类型的spark对象
        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        // 创建数据库配置
        Properties properties = new Properties();
        properties.setProperty("user", "jydb");
        properties.setProperty("password", "jydb");
        properties.setProperty("driver", "com.mysql.cj.jdbc.Driver");

        Dataset<Row> bondStream = sparkSession.read()
                .jdbc("jdbc:mysql://192.168.71.28:33061/jydb", "bond_basicinfon", properties)
                .select("InnerCode", "*").limit(500);

        //输出数据
        bondStream.show();
        //输出表的结构
//        bondStream.printSchema();
        sparkSession.stop();
    }
}
