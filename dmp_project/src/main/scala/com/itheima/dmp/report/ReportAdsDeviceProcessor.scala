package com.itheima.dmp.report

import com.itheima.dmp.`trait`.ReportProcessor
import com.itheima.dmp.config.AppConfigHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object ReportAdsDeviceProcessor extends ReportProcessor{

  override val tableName: String = AppConfigHelper.REPORT_ADS_DEVICE_TABLE_NAME

  override val keys: Seq[String] = Seq("report_date", "client", "device")

  /**
    * 真正报表分析、处理的方法
    *
    * @param odsDF
    * @return
    */
  override def realProcessData(odsDF: DataFrame): DataFrame = {

    val spark: SparkSession = odsDF.sparkSession

    //将df缓存
    odsDF.persist(StorageLevel.MEMORY_AND_DISK)
    odsDF.count() //触发缓存

    //创建临时试图
    odsDF.createOrReplaceTempView("view_ads_device")

    val groupFields: Seq[String] = Seq("client", "device")

    //执行sql语句
    val reportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsKpiWithSQL("view_ads_device", groupFields))

    reportDF
  }
}
