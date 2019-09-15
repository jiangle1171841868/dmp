package com.itheima.dmp.report

import breeze.linalg.sum
import com.itheima.dmp.`trait`.ReportProcessor
import com.itheima.dmp.config.AppConfigHelper
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  def realProcessDataV1(odsDF: DataFrame): DataFrame = {

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
    //val reportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsRegionSQL("view_ads_region"))

    regionDF
  }

  /**
    * 使用with sql语句
    *
    * @param odsDF
    * @return
    */
  def realProcessDataV2(odsDF: DataFrame): DataFrame = {

    val spark = odsDF.sparkSession

    /// TODO: 使用SQl进行报表分析

    //将odsDF注册为临时视图,使用sql分析
    odsDF.createOrReplaceTempView("view_ads_region")

    //执行广告地域分布的SQL语句
    val reportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsRegionKpiWithSQL("view_ads_region"))

    reportDF

  }

  /*/**
    * 使用DSL语句
    *
    * @param odsDF
    */
  def realProcessDataV3(odsDF: DataFrame): DataFrame = {

    val spark: SparkSession = odsDF.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val reportDF: DataFrame = odsDF
      //按照(区域)省市分组
      .groupBy($"province", $"city")
      //聚合
      .agg(
      sum(
        when($"requestmode".equalTo(1).and($"processnode".geq(1)), 1).otherwise(0)
      ).as("orginal_req_cnt"),
      sum(
        when($"requestmode".equalTo(1).and($"processnode".geq(2)), 1).otherwise(0)
      ).as("valid_req_cnt"),
      sum(
        when($"requestmode".equalTo(1).and($"processnode".equalTo(3)), 1).otherwise(0)
      ).as("ad_req_cnt"),
      sum(
        when(
          $"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"isbid".equalTo(1))
            .and($"adorderid".notEqual(0))
          , 1
        ).otherwise(0)
      ).as("join_rtx_cnt"),
      sum(
        when(
          $"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
          , 1
        ).otherwise(0)
      ).as("success_rtx_cnt"),
      sum(
        when($"requestmode".equalTo(2).and($"iseffective".equalTo(1)), 1).otherwise(0)
      ).as("ad_show_cnt"),
      sum(
        when($"requestmode".equalTo(3).and($"iseffective".equalTo(1)), 1).otherwise(0)
      ).as("ad_click_cnt"),
      sum(
        when(
          $"requestmode".equalTo(2)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
          , 1
        ).otherwise(0)
      ).as("media_show_cnt"),
      sum(
        when(
          $"requestmode".equalTo(3)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
          , 1
        ).otherwise(0)
      ).as("media_click_cnt"),
      sum(
        when(
          $"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000))
            .and($"adcreativeid".gt(200000))
          , floor($"winprice" / 1000)
        ).otherwise(0)
      ).as("dsp_pay_money"),
      sum(
        when(
          $"adplatformproviderid".geq(100000)
            .and($"iseffective".equalTo(1))
            .and($"isbilling".equalTo(1))
            .and($"iswin".equalTo(1))
            .and($"adorderid".gt(200000))
            .and($"adcreativeid".gt(200000))
          , floor($"adpayment" / 1000)
        ).otherwise(0)
      ).as("dsp_cost_money")
    )
      //过滤不为0的数据
      .filter(
      $"join_rtx_cnt".notEqual(1)
        .and($"success_rtx_cnt".notEqual(0))
        .and($"ad_show_cnt".notEqual(0))
        .and($"ad_click_cnt".notEqual(0))
        .and($"media_show_cnt".notEqual(0))
        .and($"media_click_cnt".notEqual(0))
    )
      // 获取各个字段的值，计算3个率
      .select(
      current_date().cast(StringType).as("report_date"),
      $"province", $"city", $"orginal_req_cnt", $"valid_req_cnt", $"ad_req_cnt",
      $"join_rtx_cnt", $"success_rtx_cnt", $"ad_show_cnt", $"ad_click_cnt",
      $"media_show_cnt", $"media_click_cnt",
      round($"success_rtx_cnt" / $"join_rtx_cnt", 2).as("success_rtx_rate"),
      round($"ad_click_cnt" / $"ad_show_cnt", 2).as("ad_click_rate"),
      round($"media_click_cnt" / $"media_show_cnt", 2).as("media_click_rate")
    )
    reportDF
  }
*/
  override def realProcessData(odsDF: DataFrame): DataFrame = {

    val spark = odsDF.sparkSession

    val groupFields: Seq[String] = Seq("province", "city")
    /// TODO: 使用SQl进行报表分析
    //需要多次使用odsDF,将odsDF缓存
    odsDF.persist(StorageLevel.MEMORY_AND_DISK)

    //将odsDF注册为临时视图,使用sql分析
    odsDF.createOrReplaceTempView("view_ads_region")

    //执行广告地域分布的SQL语句
    val reportDF: DataFrame = spark.sql(ReportSQLConstant.reportAdsKpiWithSQL("view_ads_region", groupFields))
    reportDF

  }


}
