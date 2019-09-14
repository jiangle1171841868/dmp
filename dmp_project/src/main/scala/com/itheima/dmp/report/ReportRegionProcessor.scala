package com.itheima.dmp.report

import com.itheima.dmp.`trait`.ReportProcessor
import com.itheima.dmp.config.AppConfigHelper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType

object ReportRegionProcessor extends ReportProcessor {

  override val tableName: String = AppConfigHelper.REPORT_REGION_STAT_TABLE_NAME

  override val keys: Seq[String] = Seq("report_date", "province", "city")

  /**
    * 真正报表分析、处理的方法
    *
    * @param odsDF
    * @return
    */
  override def realProcessData(odsDF: DataFrame): DataFrame = {

    val spark = odsDF.sparkSession

    /// TODO: 按照日期、省市进行分组,并统计各地域分布 -> groupBy
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val reportDF: DataFrame = odsDF
      //a.分组
      .groupBy($"province", $"city")
      //b.组内计数,字段名就是count
      .count()
      //c.获取报表数据  -> select
      .select(
      //时间戳  kudu里面没有字符串-> 将时间转化为字符串
      current_date().cast(StringType).as("report_date"),
      $"province", $"city", $"count"
    )

    //返回报表
    reportDF
  }
}
