package com.fl.spark.core.scala.jobs

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties


/**
 * @author fl37804
 * @date 2022-07-20
 */
object ReadMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("real mysql data").getOrCreate()
    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    val url = "jdbc:mysql://localhost:3306/fl?serverTimezone=UTC"

    val tableName = "lc_auditopinion"

    val props = new Properties()
    props.put("driver", "com.mysql.cj.jdbc.Driver")
    props.put("user", "root")
    props.put("password", "123456")
    props.put("url", url)
    props.put("fetchSize", "1")


    read1(spark, url, tableName, props).show()

    read2(spark, tableName, "CompanyCode,AccountingFirms", url, 1, 10, 10, props).show()

    read3(spark, "table3", "id", url, 1, 1000, 10, props).show()

    read4(spark, url, "select * from lc_auditopinion where CompanyCode = 156939 limit 100").show()

  }


  /**
   * 单分区读，且是全量读，应用：表数据量小的本地测试  fetchSize 这个设置不生效
   *
   * @param spark
   * @param url
   * @param table
   * @param props
   * @return
   */
  def read1(spark: SparkSession, url: String, table: String, props: Properties): DataFrame = {
    spark.read.jdbc(url, table, props)
  }

  /**
   * 多分区读，但条件列必须是数值列，且limit无法下推mysql，即使给upperBound = 10 也会全量读
   */
  def read2(spark: SparkSession, table: String, column: String, url: String, lower: Long, upper: Long,
            parts: Int, props: Properties): DataFrame = {
    spark.read.jdbc(url, table, column, lower, upper, parts, props)
  }

  /**
   * 多分区读，任意条件，limit可下推mysql
   */
  def read3(spark: SparkSession, table: String, column: String, url: String, lower: Long, upper: Long, parts: Int,
            props: Properties): DataFrame = {
    val step = ((upper - lower) / parts).toInt
    val predicates: Array[String] = 1.to(parts).map(index => {
      val lowerBound = (index - 1) * step + lower
      val upperBound = index * step + lower
      column + " >=" + lowerBound + " and " + column + " < " + upperBound
    }).toArray
    predicates.foreach(println)
    spark.read.jdbc(url, table, predicates, props)
  }

  /**
   * 单分区读，可以limit，应用：不管表大小，只抽取前n条
   */
  def read4(spark: SparkSession, url: String, sql: String): DataFrame = {
    spark.read.format("jdbc")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("url", url)
      .option("dbtable", s"($sql) dbtable").load()
  }

}
