package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 省市标签生成
  */
object Tags4RegionMaker extends TagsMaker {

  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    var map = Map[String, Double]()

    /**
      * 省份标签(ProvinceTag)生成
      */
    val province = row.getAs[String]("province")

    if (StringUtils.isNotBlank(province)) {

      map += s"PN@$province"
    }

    /**
      * 城市标签(CityTag)生成
      */
    val city = row.getAs[String]("city")
    if (StringUtils.isNotBlank(city)) {

      map += s"CT@$city" -> 1.0
    }
    map
  }
}
