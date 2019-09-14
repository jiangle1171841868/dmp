package com.itheima.dmp.report

import com.itheima.dmp.`trait`.ReportProcessor
import com.itheima.dmp.config.AppConfigHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

object ReportAdsRegionProcessor extends ReportProcessor {


  override val tableName: String = AppConfigHelper.REPORT_ADS_REGION_TABLE_NAME

  override val keys: Seq[String] = Seq("report_date", "province", "city")

  /**
    * 真正报表分析、处理的方法
    *
    * @param odsDF
    * @return
    */
  override def realProcessData(odsDF: DataFrame): DataFrame = {

    val spark = odsDF.sparkSession

    /// TODO: 使用SQl进行报表分析
    //需要多次使用odsDF,将odsDF缓存
    odsDF.persist(StorageLevel.MEMORY_AND_DISK)

    //将odsDF注册为临时视图,使用sql分析
    odsDF.createOrReplaceTempView("view_region")

    //执行广告地域分布的SQL语句
    val regionDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsRegionSQL("view_region"))

    //执行竞价成功率、广告点击率、媒体点击率的SQL
    regionDF.createOrReplaceTempView("view_ads_region")
    val reportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsRegionSQL("view_ads_region"))

    reportDF
  }
}
