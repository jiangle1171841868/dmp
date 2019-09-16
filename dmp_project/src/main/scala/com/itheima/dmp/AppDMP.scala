package com.itheima.dmp

import com.itheima.dmp.config.AppConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object AppDMP {

  def main(args: Array[String]): Unit = {

    /// TODO: 1.构建sparksession对象
    val spark = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))

      //b.判断是本地模式还是集群模式运行  本地模式运行 -> 设置local[] 集群模式 -> 在提交任务的时候设置
      if (AppConfigHelper.SPARK_APP_LOCAL_MODE.toBoolean) {
        sparkConf.setMaster(AppConfigHelper.SPARK_APP_MASTER)
      }

      //b.构造者模式创建sparksession实例对象
      import com.itheima.dmp.config.SparkConfigHelper._
      val session = SparkSession.builder()
        //加载参数配置信息 隐式转换实现 配置文件的参数变化 不需要改变代码
        .loadSparkConf()
        //加载spark配置信息
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
