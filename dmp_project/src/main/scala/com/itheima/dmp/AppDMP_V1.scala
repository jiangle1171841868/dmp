package com.itheima.dmp

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppDMP_V1 {

  def main(args: Array[String]): Unit = {

    /// TODO: 加载配置文件
    val config: Config = ConfigFactory.load("spark.conf")

    /// TODO: 1.构建sparksession对象
    val spark = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")
        //设置参数
        .set("spark.worker.timeout", config.getString("spark.worker.timeout"))
        .set("spark.cores.max", config.getString("spark.cores.max"))
        .set("spark.rpc.askTimeout", config.getString("spark.rpc.askTimeout"))
        .set("spark.network.timeout", config.getString("spark.network.timeout"))
        .set("spark.task.maxFailures", config.getString("spark.task.maxFailures"))
        .set("spark.speculation", config.getString("spark.speculation"))
        .set("spark.driver.allowMultipleContexts", config.getString("spark.driver.allowMultipleContexts"))
        .set("spark.serializer", config.getString("spark.serializer"))
        .set("spark.buffer.pageSize", config.getString("spark.buffer.pageSize"))

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
