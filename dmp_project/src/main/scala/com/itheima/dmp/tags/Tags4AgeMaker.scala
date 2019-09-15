package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 年龄标签(AgeTag)生成
  */
object Tags4AgeMaker extends TagsMaker {
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    val age = row.getAs[String]("age")
    if (StringUtils.isNotBlank(age)) {
      Map(s"AGE@$age" -> 1.0)
    } else {
      Map[String, Double]()
    }
  }

}
