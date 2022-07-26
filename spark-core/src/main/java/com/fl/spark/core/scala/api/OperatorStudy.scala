package com.fl.spark.core.scala.api

/**
 * @author fl37804
 * @date 2022-07-22
 *
 *       转换算子实战
 *       map：将集合中的所有元素乘以2
 *       filter：过滤出集合中的所有的偶数
 *       flatMap：将文本行拆分多个单词
 *       groupByKey：将班级的成绩分组
 *       reduceByKey:统计每个班级的总分数
 *       sortByKey：对班级的分数进行排序 默认是升序排序
 *       join:打印每个学生的成绩
 *       cogroup:打印每个学生的成绩
 *
 */
object OperatorStudy {

  import org.apache.spark.{SparkConf, SparkContext}

  object Transform_0020_TransformPractice {
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("TransFormationPractice").setMaster("local[2]")
      val sc = new SparkContext(conf)
      //    map(sc)
      //    filter(sc)
      //    flatMap(sc)
      //    groupByKey(sc)
      //    reduceByKey(sc)
      //    sortByKey(sc)
      //    join(sc)
      corgroup(sc)
      sc.stop()

    }

    /**
     * map：将集合中的所有元素乘以2
     *
     * @param sc
     */
    def map(sc: SparkContext): Unit = {
      val numlist = List(1, 2, 3, 4, 5);
      val numberRDD = sc.parallelize(numlist)
      /**
       * 使用map算子;将集合中的每个元素都乘以2
       */
      val multipleNumberRDD = numberRDD.map(_ * 2)
      multipleNumberRDD.foreach(println)
    }

    /**
     * filter：过滤出集合中的所有的偶数
     *
     * @param sc
     */
    def filter(sc: SparkContext): Unit = {
      val numlist = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
      val numberRDD = sc.parallelize(numlist)
      /**
       * 使用map算子;将集合中的每个元素都乘以2
       */
      val multipleNumberRDD = numberRDD.filter(_ % 2 == 0)
      multipleNumberRDD.foreach(println)
    }

    /**
     * flatMap：将文本行拆分多个单词
     */
    def flatMap(sc: SparkContext): Unit = {
      val numlist = List("hello you", "hello me", "hello world");
      val numberRDD = sc.parallelize(numlist)
      /**
       * 使用map算子;将集合中的每个元素都乘以2
       */
      val multipleNumberRDD = numberRDD.flatMap(_.split(" "))
      multipleNumberRDD.foreach(println)
    }

    /**
     * groupByKey：按照班级对成绩分组
     */
    def groupByKey(sc: SparkContext): Unit = {
      val numlist = List(Tuple2("class1", 90), Tuple2("class1", 92), Tuple2("class2", 50), Tuple2("class2", 60));
      val numberRDD = sc.parallelize(numlist)
      /**
       * 使用map算子;将集合中的每个元素都乘以2
       */
      val multipleNumberRDD = numberRDD.groupByKey()
      multipleNumberRDD.foreach(println)
    }

    /**
     * reduceByKey:统计每个班级的总分数
     *
     * @param sc
     */
    def reduceByKey(sc: SparkContext): Unit = {
      val numlist = List(Tuple2("class1", 90), Tuple2("class1", 92), Tuple2("class2", 50), Tuple2("class2", 60));
      val numberRDD = sc.parallelize(numlist)
      val retRDD = numberRDD.reduceByKey(_ + _)
      retRDD.foreach(println)

    }

    /**
     * sortByKey：对班级的分数进行排序
     */
    def sortByKey(sc: SparkContext): Unit = {
      val numlist = Array(Tuple2(65, "Tom"), Tuple2(50, "jack"), Tuple2(100, "luce"), Tuple2(85, "ff"));
      val scoreRDD = sc.parallelize(numlist)
      val retValuRDD = scoreRDD.sortByKey(false)
      retValuRDD.collect.foreach(println)
    }

    /**
     * join:打印每个学生的成绩
     */
    def join(sc: SparkContext): Unit = {
      val students = Array(
        Tuple2(1, "tom"),
        Tuple2(2, "jack"),
        Tuple2(3, "marry"),
        Tuple2(4, "ff"))

      val scores = Array(
        Tuple2(1, 80),
        Tuple2(2, 90),
        Tuple2(3, 100),
        Tuple2(1, 60),
        Tuple2(2, 80))

      val studentRDD = sc.parallelize(students)
      val scoreRDD = sc.parallelize(scores)
      val retRDD = studentRDD.join(scoreRDD)
      retRDD.foreach(println)

      /**
       * (1,(tom,80))
       * (2,(jack,90))
       * (1,(tom,60))
       * (2,(jack,80))
       * (3,(marry,100))
       */

    }

    /**
     * corgroup:打印每个学生的成绩
     */
    def corgroup(sc: SparkContext): Unit = {
      val students = Array(
        Tuple2(1, "tom"),
        Tuple2(2, "jack"),
        Tuple2(3, "marry"),
        Tuple2(4, "ff"))

      val scores = Array(
        Tuple2(1, 80),
        Tuple2(2, 90),
        Tuple2(3, 100),
        Tuple2(1, 60),
        Tuple2(2, 80))

      val studentRDD = sc.parallelize(students)
      val scoreRDD = sc.parallelize(scores)
      val retRDD = studentRDD.cogroup(scoreRDD)
      retRDD.foreach(println)

      /**
       * (4,(CompactBuffer(ff),CompactBuffer()))
       * (1,(CompactBuffer(tom),CompactBuffer(80, 60)))
       * (2,(CompactBuffer(jack),CompactBuffer(90, 80)))
       * (3,(CompactBuffer(marry),CompactBuffer(100)))
       */
    }


  }
}
