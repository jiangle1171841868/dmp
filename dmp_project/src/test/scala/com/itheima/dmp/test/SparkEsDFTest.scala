package com.itheima.dmp.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person(id: Long, name: String, age: Int)

object SparkEsDFTest {

  def main(args: Array[String]): Unit = {

    // 1. 构建sparksession对象
    val (spark, sc) = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

        // b. 设置集成ES相关参数
        .set("es.nodes", "node01,node02,node03")
        .set("es.port", "9200")
        // 索引库不存在就创建
        .set("es.index.auto.create", "true")

      //b.构造者模式创建sparksession实例对象
      val session = SparkSession.builder()
        //可以传入kv配置信息  也可以直接传入sparkConfig
        .config(sparkConf)
        .getOrCreate()

      //c.返回spark实例
      (session, session.sparkContext)
    }

    import spark.implicits._
    // TODO： 数据封装在DataFrame中保存Es索引
    // a. 模拟数据，封装在Person样例类中
    val personDF = Seq(
      Person(10001, "zhangsan", 26), Person(10002, "lisi", 25),
      Person(10003, "wangwu", 24), Person(10004, "zhaoliu", 25)
    ).toDF()

    //导包
    import org.elasticsearch.spark.sql._
    personDF.saveToEs("spark-df/persons")

    /// TODO: 读取es数据
    val esDF: DataFrame = spark
      .read
      .format("es")
      .load("spark-df/persons")
    esDF.printSchema()
    esDF.show(10, truncate = false)

    val esdf: DataFrame = spark.esDF("spark-df/persons")
    esdf.printSchema()
    esdf.show(10, truncate = false)

    sc.stop()
  }
}
