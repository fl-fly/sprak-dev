package com.fl.spark.core.scala.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author fl37804
 * @date 2022-07-24
 */
object MakeRddTest1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("test rdd")

    val sc = new SparkContext(conf)

    // 设置 RDD并行度 && 分区
    // makeRdd 方法可以传递第二个参数，这个参数表示分区的数量
    // 第二个参数不传递，默认是 （当前运行的机器的）配置的 最大核数
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    // 将处理的文件保存成分区文件
    rdd.saveAsTextFile("output")

    sc.stop()

  }
}
