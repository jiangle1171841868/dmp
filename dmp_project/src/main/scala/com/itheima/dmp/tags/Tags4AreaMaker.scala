package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


/**
  * 商圈标签(AreaTag)生成
  */
object Tags4AreaMaker extends TagsMaker {
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    val area: String = row.getAs[String]("area")

    // 商圈数据中有写有数据,有写没有数据 是None  ->   需要过滤出None的数据
    if (!None.equals("area")) {
      area.split(":")
        .filter(area => StringUtils.isNotBlank(area))
        .map(area => (s"BA@$area" -> 1.0))
        .toMap
    }else{
      Map[String, Double]()
    }
  }
}
