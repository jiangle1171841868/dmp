package com.itheima.dmp.run

import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.tags.{HistoryTagsProcessor, MakeTagsProcessor, MergeTagsProcessor}
import com.itheima.dmp.utils.{DateUtils, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 1. 创建SparkSession
  * 2. 加载ods表的数据和area表的数据，并且根据GeoHash进行关联
  *    - 2.1. 加载ods表的数据
  *    - 2.2  加载area表的数据
  *    - 2.3  ods表数据与area表数据关联
  * 3. 获取当日用户标签
  * 4. 将数据保存到kudu表
  * 5. 关闭资源
  */
object DailyTagsRunner {

  def main(args: Array[String]): Unit = {

    // ODS表名
    val ODS_TABLE_NAME: String = AppConfigHelper.AD_MAIN_TABLE_NAME

    // Area的表名
    val AREA_TABLE_NAME: String = AppConfigHelper.BUSINESS_AREAS_TABLE_NAME

    //定义历史的标签数据表名
    val HISTORY_TAGS_TABLE_NAME: String = {
      AppConfigHelper.TAGS_TABLE_NAME_PREFIX + DateUtils.getYesterdayDate()
    }

    //定义当天的标签数据表名
    val TODAY_TAGS_TABLE = {
      AppConfigHelper.TAGS_TABLE_NAME_PREFIX + DateUtils.getTodayDate()
    }

    // 1. 构建sparksession实例对象
    val spark: SparkSession = SparkSessionUtils.createSparkSession(this.getClass)

    // 2. 读取kudu表的数据
    import com.itheima.dmp.utils.KuduUtils._

    // 2.1 加载ods表的数据
    val optionOdsDF: Option[DataFrame] = spark.readKuduTable(ODS_TABLE_NAME)
    val odsDF: DataFrame = optionOdsDF match {
      case Some(odsDF) => odsDF
      case None => println("ERROR: ODS表无数据,结束执行"); return
    }

    // 2.2  加载area表的数据
    val optionAreaDF: Option[DataFrame] = spark.readKuduTable(AREA_TABLE_NAME)
    val odsWithAreaDF: DataFrame = optionAreaDF match {

      // 2.3 ods表数据与area表数据关联 左关联 -> 表存在就join 不存在就直接返回odsDF
      case Some(areaDF) =>
        odsDF.join(
          areaDF, //关联的表
          odsDF.col("geoHash").equalTo(areaDF.col("geo_hash")), //关联条件,可以调用and方法设置多个条件
          "left" //关联方式
        )
      //area表不存在,返回odsDF表
      case None => odsDF
    }

    // 3. 获取当日用户标签
    val tagsDF: DataFrame = MakeTagsProcessor.processData(odsWithAreaDF)

    // 4.获取历史标签数据
    val historyTagsDFOption: Option[DataFrame] = spark.readKuduTable(HISTORY_TAGS_TABLE_NAME)

    //标签合并
    val allTagsDF: DataFrame = historyTagsDFOption match {
      case Some(historyTagsDFOption) =>
        tagsDF.union(HistoryTagsProcessor.processData(historyTagsDFOption)) //union 就相当于sql中的union all
      case None => tagsDF
    }


    // 5. 用户统一统一识别  ->  合并标签
    val userTagsDF: DataFrame = MergeTagsProcessor.processData(allTagsDF)
    //userTagsDF.printSchema()

    //. 将数据保存到kudu表
    // a. 创建kudu表,存在不删除,主键是main_id
    spark.createKuduTable(TODAY_TAGS_TABLE, userTagsDF.schema, Seq("main_id"), isDelete = false)

    // b. 将数据保存到kudu表
    userTagsDF.saveAsKuduTable (TODAY_TAGS_TABLE)

    // 将数据保存到ES中
    EsUtils.saveJsonToEsIndex(userTagsDF, s"dmp_user_tags2/tags_${DateUtils.getTodayDate()}")

    // 6. 关闭资源
    spark.stop()
  }

}
