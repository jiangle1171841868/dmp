package com.itheima.dmp.run

import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.report.{ReportAdsRegionProcessor, ReportRegionProcessor}
import com.itheima.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 生成报表的主类
  * 1.构建SparkSession
  * 2.加载ods数据
  * 3.处理数据
  * 4.应用完成之后,关闭资源
  */
object DailyReportRunner {

  def main(args: Array[String]): Unit = {

    /// TODO: 1.构建SparkSession
    val spark: SparkSession = SparkSessionUtils.createSparkSession(this.getClass)

    /// TODO: 2.加载ods数据
    import com.itheima.dmp.utils.KuduUtils._
    val odsTableName: String = AppConfigHelper.AD_MAIN_TABLE_NAME
    val odsDataFrame: Option[DataFrame] = spark.readKuduTable(odsTableName)
    val odsDF = odsDataFrame match {
      case Some(df) => df
      case None => println("查询的数据不存在..."); return
    }

    /// TODO: 3.处理数据
    // TODO: 不同业务类型报表分析，并将结果数据存储Kudu表中
    // a. 各地域分布统计：region_stat_analysis
    //ReportRegionProcessor.processData(odsDF)
    // b. 广告区域统计：ads_region_analysis
    ReportAdsRegionProcessor.processData(odsDF)
    // c. 广告APP统计：ads_app_analysis
    //ReportAdsAppProcessor.processData(odsDF)
    // d. 广告设备统计：ads_device_analysis
    //ReportAdsDeviceProcessor.processData(odsDF)
    // e. 广告网络类型统计：ads_network_analysis
    //ReportAdsNetworkProcessor.processData(odsDF)
    // f. 广告运营商统计：ads_isp_analysis
    //ReportAdsIspProcessor.processData(odsDF)
    // g. 广告渠道统计：ads_channel_analysis
    //ReportAdsChannelProcessor.processData(odsDF)

    Thread.sleep(1000000)

    /// TODO: 4.应用完成之后,关闭资源
    spark.stop()


  }


}
