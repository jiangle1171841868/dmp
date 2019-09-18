package com.itheima.dmp.utils

import com.itheima.dmp.beans.UserTags
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 将构建用户标签数据保存到ElasticSearch中
  */
object EsUtils {

  /**
    * 将DataFrame数据转换为Map集合，存储到ElasticSearch索引Index
    */
  def saveMapToEsIndex(dataFrame: DataFrame, resource: String): Unit = {

    // 获取SparkSession实例对象，导入隐式转换
    val spark: SparkSession = dataFrame.sparkSession
    import spark.implicits._

    // 1. 将用户标签DataFrame ->  DataSet  -> RDD  -> 将数据封装到Map集合中 -> RDD

    val tagsMapRDD = dataFrame.as[UserTags].rdd
      .mapPartitions { iter =>
        iter.map { case UserTags(mainId, ids, tags) =>

          // a. 将ids转化为Map集合
          val idsMap: Map[String, String] = TagUtils.idsStr2Map(ids)

          // b. 将tags转化为Map集合
          val tagsMap: Map[String, Double] = TagUtils.tagsStr2Map(tags)

          // 数据格式   ->  (KW@丰田->1.0),(KW@二手车->1.0)   转化后的格式  ->  KW -> 丰田:1.0,二手车:1.0
          val saveTagsMap: Map[String, String] = tagsMap
            .toList //Map集合中的数据,key重复了就会被覆盖 -> 转化为List集合
            .map { case (tag, tagWeight) =>

            // 获取前缀KW
            val Array(tagName, tagValue) = tag.split("@")

            // 返回新的二元组
            tagName -> s"$tagValue:$tagWeight"
          }

            // c. 按照标签的前缀分组,保存到es中是以前缀为字段名保存数据
            .groupBy(_._1)
            //将每个组内的value组合,拼接成一个字符串
            .map { case (kewWord, list) =>

            kewWord -> list.map(_._2).mkString(",")
          }

          // ids和tags集合合并,返回Map集合
          idsMap ++ saveTagsMap
        }
      }
  }


  /**
    * 将DataFrame数据转换为JSON格式数据，存储到ElasticSearch索引Index
    */
  def saveJsonToEsIndex(dataFrame: DataFrame, resource: String): Unit = {

  }
}
