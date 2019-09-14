package com.itheima.dmp.utils

import com.itheima.dmp.config.AppConfigHelper
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/// TODO: SparkSession工具类
object SparkSessionUtils {

  /**
    * SparkSession工具类:创建SparkSession实例对象
    * @param appClass : 传入调用方法类的字节码对象,用来设置应用名字AppName
    * @return
    */
  def createSparkSession(appClass: Class[_]): SparkSession = {
    /// TODO: 1.构建sparksession对象

      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(appClass.getSimpleName.stripSuffix("$"))
      // 判断是本地模式还是集群模式运行  本地模式运行 -> 设置local[] 集群模式 -> 在提交任务的时候设置
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
}
