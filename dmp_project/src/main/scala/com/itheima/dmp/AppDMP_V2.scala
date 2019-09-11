package com.itheima.dmp

import com.itheima.dmp.config.AppConfigHelper
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppDMP_V2 {

  def main(args: Array[String]): Unit = {


    /// TODO: 1.构建sparksession对象
    val spark = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
        //设置所有参数
        /**
          * def setAll
          * (
          * settings: Traversable[(String, String)]  //todo Traversable 是scala集合中的父类
          * ): SparkConf
          *
          * 配置参数都封装在list集合中 直接将list集合添加进去
          */
        .setAll(AppConfigHelper.SPARK_APP_PARAMS)

      //b.构造者模式创建sparksession实例对象
      val session = SparkSession.builder()
        //可以传入kv配置信息  也可以直接传入sparkConfig
        .config(sparkConf)
        .getOrCreate()

      //c.返回spark实例
      session
    }

    Thread.sleep(100000)

    /// TODO: 关闭资源
    spark.stop()


  }

}
