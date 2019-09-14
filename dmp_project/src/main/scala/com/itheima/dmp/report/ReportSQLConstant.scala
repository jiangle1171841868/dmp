package com.itheima.dmp.report

//广告报表的SQl语句
object ReportSQLConstant {

  /**
    * 广告地域分布的SQL语句
    *
    * @param tempViewName
    * @return
    */
  def reportAdsRegionSQL(tempViewName: String): String = {

    s"""
       		  |SELECT
       		  |	 CAST(TO_DATE(NOW()) AS STRING) AS report_date,
       		  |  province, city,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 1 AND processnode >= 1 THEN 1 ELSE 0 END
       		  |  )AS orginal_req_cnt,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 1 AND processnode >= 2 THEN 1 ELSE 0 END
       		  |  )AS valid_req_cnt,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 1 AND processnode = 3 THEN 1 ELSE 0 END
       		  |  )AS ad_req_cnt,
       		  |  SUM(
       		  |     CASE WHEN adplatformproviderid >= 100000 AND iseffective = 1 AND isbilling = 1 AND isbid = 1 AND adorderid != 0 THEN 1 ELSE 0 END
       		  |  )AS join_rtx_cnt,
       		  |  SUM(
       		  |     CASE WHEN adplatformproviderid >= 100000 AND iseffective = 1 AND isbilling = 1 AND iswin = 1 THEN 1 ELSE 0 END
       		  |  )AS success_rtx_cnt,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 2 AND iseffective = 1 THEN 1 ELSE 0 END
       		  |  )AS ad_show_cnt,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 3 AND iseffective = 1 THEN 1 ELSE 0 END
       		  |  )AS ad_click_cnt,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 2 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0 END
       		  |  )AS media_show_cnt,
       		  |  SUM(
       		  |     CASE WHEN requestmode = 3 AND iseffective = 1 AND isbilling = 1 THEN 1 ELSE 0 END
       		  |  )AS media_click_cnt,
       		  |  SUM(
       		  |     CASE WHEN adplatformproviderid >= 100000 AND iseffective = 1 AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND adcreativeid > 200000 THEN floor(winprice/1000) ELSE 0 END
       		  |  )AS dsp_pay_money,
       		  |  SUM(
       		  |     CASE WHEN adplatformproviderid >= 100000 AND iseffective = 1 AND isbilling = 1 AND iswin = 1 AND adorderid > 200000 AND adcreativeid > 200000 THEN floor(adpayment/1000) ELSE 0 END
       		  |  )AS dsp_cost_money
       		  |FROM
       		  |  $tempViewName
       		  |GROUP BY
       		  |  province, city
		""".stripMargin

  }

  /**
    * 统计竞价成功率、广告点击率、媒体点击率的SQL
    */
  def reportAdsRegionRateSQL(reportViewName: String): String = {
    s"""
       		   |SELECT
       		   |  t.*,
       		   |  round(t.success_rtx_cnt / t.join_rtx_cnt, 2) AS success_rtx_rate,
       		   |  round(t.ad_click_cnt / t.ad_show_cnt, 2) AS ad_click_rate,
       		   |  round(t.media_click_cnt / t.media_show_cnt, 2) AS media_click_rate
       		   |FROM
       		   |  $reportViewName t
       		   |WHERE
       		   |  t.join_rtx_cnt != 0 AND t.success_rtx_cnt != 0
       		   |  AND
       		   |  t.ad_show_cnt != 0 AND t.ad_click_cnt != 0
       		   |  AND
       		   |  t.media_show_cnt != 0 AND t.media_click_cnt != 0
      """.stripMargin
  }

}
