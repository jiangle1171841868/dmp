package com.itheima.dmp.`trait`

import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
  * 报表处理接口:抽取报表处理的相同步骤
  * 1.实体方法 -> processData:数据处理全过程的方法
  *    - 数据处理的方法 -> 每个报表不相同 单独抽取出来 -> realProcessData
  *    - 数据存储的方法  -> 表名和keys不一样  -> 抽取出报名和keys 为抽象字段
  */
trait ReportProcessor {

  //表名
  val tableName: String

  //keys
  val keys: Seq[String]


  def processData(odsDF: DataFrame): Unit = {

    val spark: SparkSession = odsDF.sparkSession

    //1.调用实际报表处理的方法   ->  处理数据
    val reportDF: DataFrame = realProcessData(odsDF)

    //2.将数据保存到kudu表中
    import com.itheima.dmp.utils.KuduUtils._
    //a.创建表，如果表存在不删除不创建 -> 表名和keys是变化的 -> 抽取出来作为抽象字段
    spark.createKuduTable(tableName, reportDF.schema, keys, isDelete = false)
    //b.保存数据 -> 报表处理后的数据比较少  考虑降低分区数
    reportDF.coalesce(1).saveAsKuduTable(tableName)

  }

  /**
    * 真正报表分析、处理的方法
    *
    * @param odsDF
    * @return
    */
  def realProcessData(odsDF: DataFrame): DataFrame

}
