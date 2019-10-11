package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.beans.{IdsWithTags, UserTags}
import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.utils.TagUtils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable

/**
  * 今日ODS表和AREA表生成今日用户标签
  */
object MakeTagsProcessor extends Processor {

  /**
    * 生成标签数据：广告标识、渠道、关键词、省市、性别、年龄、商圈、App名称和设备
    *
    **/
  override def processData(odsAreaDF: DataFrame): DataFrame = {

    val spark = odsAreaDF.sparkSession
    val sc: SparkContext = spark.sparkContext

    // 导入隐式转换和函数包
    import spark.implicits._
    /**
      *
      * 遍历用户的ods信息,经过表连接此时已经有了商圈信息
      *    - a. 提取各个标签属性值，求得标签集合
      *    - b. 获取id集合
      *    - c. 获取主ID
      *    - d. 返回样例类
      **/

    //加载字典数据,使用SparkContext中广播变量将字段广播到Exexutor内存中

    // a. 获取字段数据
    val appDicBroadcast: Broadcast[Map[String, String]] = {
      val appMap: Map[String, String] = spark.read.textFile(AppConfigHelper.APP_NAME_DIC)
        .map { data =>
          val Array(appId, appName) = data.split("##")
          //返回二元组
          (appId, appName)
        }.rdd.collectAsMap().toMap

      //使用sparkContext进行广播
      sc.broadcast(appMap)
    }

    val deviceDicBroadcast: Broadcast[Map[String, String]] = {
      val decviceMap = spark.read.textFile(AppConfigHelper.DEVICE_DIC)
        .map { data =>
          val Array(appId, appName) = data.split("##")
          //返回二元组
          (appId, appName)
        }.rdd.collectAsMap().toMap

      //使用sparkContext进行广播
      sc.broadcast(decviceMap)
    }

    //对dataFrame中每个分区的数据进行标签化操作
    val tagsDF: RDD[IdsWithTags] = odsAreaDF.rdd.mapPartitions { rows =>

      rows.map {
        row =>
          // 其一：构建集合Map对象，用于存放标签数据
          var tagsMap: mutable.Map[String, Double] = mutable.Map[String, Double]()

          // 1). 广告类型标签（Tags4AdTypeMaker）
          tagsMap ++= Tags4AdTypeMaker.make(row)
          // 2). 渠道标签（Tags4ChannelMaker）
          tagsMap ++= Tags4ChannelMaker.make(row)
          // 3). 关键词标签（Tags4KeyWordsMaker)
          tagsMap ++= Tags4KeyWordsMaker.make(row)
          // 4). 省份城市标签（Tags4RegionMaker）
          tagsMap ++= Tags4RegionMaker.make(row)
          // 5). 性别标签（Tags4GenderMaker）
          tagsMap ++= Tags4GenderMaker.make(row)
          // 6). 年龄标签（Tags4AgeMaker）
          tagsMap ++= Tags4AgeMaker.make(row)
          // 7). 商圈标签（Tags4AreaMaker）
          tagsMap ++= Tags4AreaMaker.make(row)
          // 8). App标签（Tags4AppMaker）
          tagsMap ++= Tags4AppMaker.make(row, appDicBroadcast.value)
          // 9). 设备标签（Tags4DeviceMaker）
          tagsMap ++= Tags4DeviceMaker.make(row, deviceDicBroadcast.value)

          // 其二：每条数据标识符，用字段：uuid
          val mainId = row.getAs[String]("uuid")

          // 其三：获取每条数据中所有标识符IDs的值
          val idsMap: Map[String, String] = getIds(row)

          // 版本V1返回值    ->   将Map转换为String类型,封装为RDD[UserTags]  再转化为DataFrame-> 因为kudu不支持Map集合,所以转换为字符串
          //UserTags(mainId, TagUtils.map2Str(idsMap), TagUtils.map2Str(tagsMap.toMap))

          // 版本V2返回值      ->   今日标签数据处理之后不直接保存带kudu表,为了方便处理,封装Map集合
          IdsWithTags(mainId, idsMap, tagsMap.toMap)
      }

    }
    tagsDF.toDF()
  }

  /**
    * 每一行数据，不一定所有的id都有值，根据id的名称来查询值，并且过滤掉不含值的数据
    *
    * @param row DataFrame中每行数据
    */
  def getIds(row: Row): Map[String, String] = {

    // a. 获取所有标识符字段名称
    val ids: Array[String] = AppConfigHelper.ID_FIELDS.split(",")

    // b. 依据标识符Id名称，获取对应值
    ids
      .map(id => id -> row.getAs[String](id))
      .filter { case (_, idValue) => StringUtils.isNotBlank(idValue) }
      .toMap
  }
}
