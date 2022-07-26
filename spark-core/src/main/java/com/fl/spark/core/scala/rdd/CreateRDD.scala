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
object CreateRDD {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("rdd test").setMaster("local")
    val sc = new SparkContext(conf)

    // 内存中创建rdd 将内存中的数据作为处理的数据源
    val seq = Seq[Int](1, 2, 3, 4, 5, 6)

    // 两种创建RDD 的方法
    val rdd = sc.parallelize(seq)

    // 这make rdd 底层就是 调用上面的方法
    //    val value = sc.makeRDD(seq)

    rdd.collect().foreach(println)

    // 关闭环境
    val unit = sc.stop()

  }
}
