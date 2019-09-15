package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.spark.sql.Row

/**
  * 性别标签(GenderTag)生成
  */
object Tags4GenderMaker extends TagsMaker {


  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    // 获取性别信息
    val sex = row.getAs[String]("sex")

    // 计算并返回标签
    sex match {
      case "1" => Map(s"GD@男" -> 1.0)
      case "0" => Map(s"GD@女" -> 1.0)
      case _ => Map()
    }

  }
}
