package com.fl.spark.core.scala.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author feng_lei.fl
 * @date: 2022-01-23 21:18
 * @description: 字数统计
 */
object WorldCount {
  def main(args: Array[String]): Unit = {

    // 1. 环境基础配置
    val conf = new SparkConf()
      // 设置spark环境为本地
      .setMaster("local")
      // 执行任务名称
      .setAppName("WordCount")

    // 2. 建立spark 框架的连接
    val sc = new SparkContext(conf)

    // 3.执行业务操作
    val lines: RDD[String] = sc.textFile("datas/*")
    val words = lines.flatMap(_.split(" "))
    // 把数据根据单次分组
    val wordGroup = words.groupBy(word => word)

    val worldCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 字数统计
    val array = worldCount.collect()
    // 打印输出
    array.foreach(println)

    // 4.关闭链接
    sc.stop()
  }
}
