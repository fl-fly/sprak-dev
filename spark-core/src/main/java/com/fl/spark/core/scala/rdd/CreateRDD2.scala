package com.fl.spark.core.scala.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author fl37804
 * @date 2022-07-21
 *
 *       创建rdd的方式有多种
 *       从文件读取、从数据源获取、手动创建
 *       步骤都是：
 *       1、创建sparkconf进行配置
 *       2、创建JavaSparkContext
 *       3、创建JavaRDD
 *       注意：SparkSession是用在SparkSQL、SparkStreaming上
 *
 */
object CreateRDD2 {
  def main(args: Array[String]): Unit = {
    // local 设置的是单线程启动  local[*] 是根据服务器来决定，如果是8核，那就启用8个线程
    val conf = new SparkConf().setAppName("rdd test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val linesRDD = sc.textFile("F:\\sprak-wrok\\sprak-dev\\spark-core\\src\\main\\resources\\data\\fl.txt")
    val wordsRDD = linesRDD.flatMap(line => line.split(","))
    val wordTuple = wordsRDD.map(word => (word, 1))
    val retRdd = wordTuple.reduceByKey((a: Int, b: Int) => (a + b))
    retRdd.foreach(println)
  }
}
