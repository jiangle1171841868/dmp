package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object Tags4KeyWordsMaker extends TagsMaker {

  /**
    * 关键词标签(KeywordTag)生成  -> 多个值
    */
  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    row.getAs[String]("keywords")
      .split(",")
      .filter(kw => StringUtils.isNotBlank(kw))
      .map { word => (s"KW@$word" -> 1.0) }
      .toMap //转化为不可变map
  }

}
