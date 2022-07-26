package com.fl.spark.core.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author feng_lei.fl
 * @date: 2022-01-23 21:18
 * @description: 字数统计
 */
object TestRdd {
  def main(args: Array[String]): Unit = {

    // 1. 环境基础配置
    val conf = new SparkConf()
      // 设置spark环境为本地
      .setMaster("local")
      // 执行任务名称
      .setAppName("WordCount")

    // 2. 建立spark 框架的连接
    val sc = new SparkContext(conf)
    // 从集合中创建 RDD，Spark 主要提供了两个方法：parallelize 和 makeRDD

    val value = sc.parallelize(List(1, 2, 3, 4))

    val value1 = sc.makeRDD(List(1, 2, 3, 4))

    value.collect().foreach(println)

    value1.collect().foreach(println)

    // 4.关闭链接
    sc.stop()
  }
}
