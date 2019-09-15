package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tags4ChannelMaker extends TagsMaker {

  /**
    * 渠道标签(ChannelTag)生成
    */
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    // 1. 从row中获取 channelid
    val channelid: String = row.getAs[String]("channelid")

    // 2. 转化 -> CH@$channelid -> 1.0
    if (StringUtils.isNotBlank(channelid)) {

      Map(s"CH@$channelid" -> 1.0)
    } else {
      Map[String, Double]()
    }
  }
}
