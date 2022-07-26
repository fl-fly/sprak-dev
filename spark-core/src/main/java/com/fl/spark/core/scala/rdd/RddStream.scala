package com.fl.spark.core.scala.rdd

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @author fl37804
 * @date 2022-06-07
 */
object RddStream {
  //  循环创建几个 RDD，将 RDD 放入队列。通过 SparkStream 创建 Dstream，计算
  def main(args: Array[String]): Unit = {
    // 初始化spark 配置
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")

    //初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(4))

    //3.创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]()

    //4.创建 QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, oneAtATime = false)

    //5.处理队列中的 RDD 数据
    val mappedStream = inputStream.map((_, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    //6.打印结果
    reducedStream.print()

    //7.启动任务
    ssc.start()

    //8.循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
  }

}
