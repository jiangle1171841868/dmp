package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * App名称标签(AppNameTag)生成
  */
object Tags4AppMaker extends TagsMaker {

  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    //获取应用id
    val appId: String = row.getAs[String]("appid")

    //通过字典获取应用appName
    val appName: String = dic.getOrElse("appid", appId)

    if (StringUtils.isNotBlank(appName)) {

      Map(s"APP@$appName" -> 1.0)
    } else {
      Map[String, Double]()
    }
  }

}
