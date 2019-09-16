package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 广告类型标签
  */
object Tags4AdTypeMaker extends TagsMaker {

  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    // 1. 从row字段中获取  adspacetype
    val adspacetype: Long = row.getAs[Long]("adspacetype")

    // 2. 将广告类型转化广告类型名称 -> 广告位类型（1：banner 2：插屏 3：全屏）->  封装成map集合返回  -> Map(标签 -> 权重)
    //先判断获取的数据是否为空
    if (StringUtils.isNotBlank(adspacetype.toString)) {
      adspacetype match {
        case 1 => Map("AD@banner" -> 1.0)
        case 2 => Map("AD@插屏" -> 1.0)
        case 3 => Map("AD@全屏" -> 1.0)
      }
    } else {
      Map[String, Double]()
    }
  }
}
