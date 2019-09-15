package com.itheima.dmp.run

import com.itheima.dmp.area.AreaProcessor
import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 根据经纬度,调用高德API,获取商圈信息,保存到kudu表中 -> 将每一天的数据追加到kudu表
  * 1. 构建sparksession实例对象
  * 2. 读取kudu表的数据
  * 3. 抽取方法 -> 根据经纬度获取商圈数据
  * 4. 保存到kudu表
  * 4. 关闭资源
  */
object BusinessAreasRunner {

  def main(args: Array[String]): Unit = {

    val SOURCE_TABLE_NAME = AppConfigHelper.AD_MAIN_TABLE_NAME
    val SINK_TABLE_NAME = AppConfigHelper.BUSINESS_AREAS_TABLE_NAME

    // 1. 构建sparksession实例对象
    val spark: SparkSession = SparkSessionUtils.createSparkSession(this.getClass)

    // 2. 读取kudu表的数据
    import com.itheima.dmp.utils.KuduUtils._
    val optionDF: Option[DataFrame] = spark.readKuduTable(SOURCE_TABLE_NAME)
    val odsDF: DataFrame = optionDF match {
      case Some(odsDF) => odsDF
      case None => println("ERROR: ODS表无数据,结束执行"); return
    }

    // 3. 抽取方法 -> 根据经纬度获取商圈数据
    val businessAreasDF: DataFrame = AreaProcessor.processData(odsDF)

    // 4. 保存到kudu表,每天的数据追加到表中
    // a. 创建kudu表,存在不删除,主键是 -> geo_hash
    spark.createKuduTable(SINK_TABLE_NAME, businessAreasDF.schema, Seq("geo_hash"), isDelete = false)

    // b. 将商圈数据保存到kudu表
    businessAreasDF.saveAsKuduTable(SINK_TABLE_NAME)

    // 5.. 关闭资源
    spark.stop()

  }

}
