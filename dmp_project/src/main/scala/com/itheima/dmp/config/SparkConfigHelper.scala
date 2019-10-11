package com.itheima.dmp.config

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.apache.spark.sql.SparkSession

/**
  * 隐式转换:将SparkSession.Builder转化为SparkConfigHelper  调用SparkConfigHelper的方法
  * 目的: 动态加载数据 配置信息改动 代码不用修改
  * 实现:谁调用方法 就是将谁转化
  */
//定义主构造器
class SparkConfigHelper(sparkBuild: SparkSession.Builder) {

  /// TODO: 1.加载数据
  val config: Config = ConfigFactory.load("spark.conf")

  /// TODO: 2.封装方法 读取参数封装到spark中
  def loadSparkConf(): SparkSession.Builder = {

    // a. 获取配置文件信息 获取的是java中的set集合,保证唯一性
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()

    // b. 遍历集合  将java集合转化为scala
    import scala.collection.JavaConverters._
    for (entry <- entrySet.asScala) {

      // c. 获取每一个entry来源的名字 获取参数封装到spark中
      /**
        *   - 获取每一个entry来源的名字
        *   - 会读取系统配置文件 和 spark.conf 中的配置文件 需要过滤出spark.conf 的配置
        *   - 系统配置文件  -> 来源是null
        *   - spark.conf   -> 配置名字是spark.conf
        */
      val resourceName = entry.getValue.origin().resource()

      /// TODO:  注意: 系统属性是的来源是null 如果使用configResource调用equals()方法 就会空指针异常  所以使用字符串类调用方法 也可以configResource == "spark.conf"
      if ("spark.conf".equals(resourceName)) {

        // d. 获取参数 封装到spark中 直接获取的value是使用Config封装的 Config(value) 使用unwrapped()拆箱 获取value
        sparkBuild.config(entry.getKey, entry.getValue.unwrapped().toString)
      }
    }
    sparkBuild
  }

}

object SparkConfigHelper {

  //隐式转换 SparkSession.Builder  ->   SparkConfigHelper
  //参数:调用别人方法的对象 就是调用隐式转换方法的对象
  implicit def SparkBuilderToSparkConfigHelper(sparkBuild: SparkSession.Builder): SparkConfigHelper = {

    //返回SparkConfigHelper  需要参数sparkBuild 通过构造方法 sparkBuild实例对象
    new SparkConfigHelper(sparkBuild)
  }

}