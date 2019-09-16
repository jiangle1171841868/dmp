package com.itheima.dmp.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object PartitionTest {

  def main(args: Array[String]): Unit = {

    // DataFrame的分区数
    /// TODO: 1.构建sparksession对象
    val (spark,sc) = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local")


      //b.构造者模式创建sparksession实例对象
      val session = SparkSession.builder()
        //可以传入kv配置信息  也可以直接传入sparkConfig
        .config(sparkConf)
        //设置默认分区数
        //.config("spark.default.parallelism", 20)
        .getOrCreate()

      //c.返回spark实例
      (session,session.sparkContext)
    }

    //读取数据,封装成DF
    val dataFrame: DataFrame = spark.read.text("datas/dic_app.data")

    //打印分区数
    println(dataFrame.rdd.getNumPartitions)


    val sourceRDD: RDD[String] = sc.textFile("datas/dic_app.data")
    println(sourceRDD.getNumPartitions)

    Thread.sleep(100000)

    /// TODO: 关闭资源
    spark.stop()

  }

}
