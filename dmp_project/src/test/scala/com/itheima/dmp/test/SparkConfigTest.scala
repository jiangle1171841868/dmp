package com.itheima.dmp.test

import java.util
import java.util.Map

import com.typesafe.config.{Config, ConfigFactory, ConfigValue}

/// TODO: 实现动态读取配置文件  配置文件改动  代码不用改动
object SparkConfigTest {

  def main(args: Array[String]): Unit = {

    /// TODO: 1.加载数据
    val config: Config = ConfigFactory.load("spark.conf")

    /// TODO: 2.获取配置文件中的属性信息 获取的是java的集合
    val entrySet: util.Set[Map.Entry[String, ConfigValue]] = config.entrySet()

    /// TODO: 3.遍历集合
    import scala.collection.JavaConverters._
    for (entry <- entrySet.asScala) {

      /**
        * 读取数据产生的问题
        *     - 1.spark.worker.timeout  -> ConfigString("500")
        *          - 解决:需要对value拆箱去掉Config 调用unwrapped()方法
        *     - 2.读取的数据除了spark.conf的数据还有系统的配置信息
        *          - 解决:获取属性的来源 过滤出spark.conf的数据
        */

      // a. 获取数据来源
      val configResource: String = entry.getValue.origin().resource()
      //println(configResource)

      /// TODO:  注意: 系统属性是的来源是null 如果使用configResource调用equals()方法 就会空指针异常  所以使用字符串类调用方法 也可以configResource == "spark.conf"
      if ("spark.conf".equals(configResource)) {

        // b.调用unwrapped() 拆箱去掉Config的封装
        println(s"${entry.getKey}  -> ${entry.getValue.unwrapped()}")
      }
    }

  }
}
