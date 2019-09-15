package com.itheima.dmp.`trait`

import org.apache.spark.sql.Row

/**
  * 依据数据生成各个标签的统一接口Trait
  */
trait TagsMaker {

  // 每行数据Row，产生对应的标签  第二个参数为一个字典，
  def make(row: Row, dic: Map[String, String] = null): Map[String, Double]

}
