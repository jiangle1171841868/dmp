package com.itheima.dmp.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object SparkTest {


  def main(args: Array[String]): Unit = {

    // 1. 构建sparksession对象
    val (spark, sc) = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

      //b.构造者模式创建sparksession实例对象
      val session = SparkSession.builder()
        //可以传入kv配置信息  也可以直接传入sparkConfig
        .config(sparkConf)
        .getOrCreate()

      //c.返回spark实例
      (session, session.sparkContext)
    }

    import spark.implicits._
    import org.apache.spark.sql.functions._
    // TODO： 数据封装在DataFrame中保存Es索引
    // a. 模拟数据，封装在Person样例类中
    val personDF =
    Seq(
      ("一班", "语文", "张三", 90),
      ("一班", "语文", "李四", 80),
      ("一班", "语文", "王五", 99),
      ("二班", "语文", "张三", 90),
      ("二班", "语文", "李四", 80),
      ("二班", "语文", "王五", 95),
      ("一班", "数学", "张三", 95),
      ("一班", "数学", "李四", 70),
      ("一班", "数学", "王五", 95),
      ("二班", "数学", "张三", 80),
      ("二班", "数学", "李四", 80),
      ("二班", "数学", "王五", 90)
    )
      .toDF("class", "cource", "name", "score")

    personDF.printSchema()
    personDF.show(10)
    personDF
      .select($"class", $"cource", $"name", $"score")
      .groupBy($"class", $"cource")
      .agg(
        max($"score").as("max_score")
      )
      // .select($"class", $"cource", $"name", $"max_score")
      .show(10)


    val rdd: RDD[(String, String, String, Int)] = sc.parallelize(Seq(
      ("一班", "语文", "张三", 90),
      ("一班", "语文", "李四", 80),
      ("一班", "语文", "王五", 99),
      ("二班", "语文", "张三", 90),
      ("二班", "语文", "李四", 80),
      ("二班", "语文", "王五", 95),
      ("一班", "数学", "张三", 95),
      ("一班", "数学", "李四", 70),
      ("一班", "数学", "王五", 95),
      ("二班", "数学", "张三", 80),
      ("二班", "数学", "李四", 80),
      ("二班", "数学", "王五", 90)
    ))

    val rdd2: RDD[((String, String), (String, Int))] = rdd
      .map { tuple =>

        ((tuple._1, tuple._2), (tuple._3, tuple._4))

      }

    rdd2
      .sortByKey()
      .take(1)
      .foreach(println)

    sc.stop()

  }
}
