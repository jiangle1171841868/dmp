package com.itheima.dmp.utils


import com.itheima.dmp.beans.UserTags
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

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

    import org.elasticsearch.spark._

    //  将用户标签DataFrame ->  DataSet  -> RDD  -> 将数据封装到Map集合中 -> RDD

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

            // c. 按照标签的前缀分组,保存到es中是以前缀为字段名保存数据  ->  保存数据格式   标签前缀   ->  标签名:标签权重集合中所有数据拼接的字符串
            .groupBy(_._1)
            //将每个组内的value组合,拼接成一个字符串
            .map { case (kewWord, list) =>

            kewWord -> list.map(_._2).mkString(",")
          }

          // ids和tags集合合并,返回Map集合
          idsMap ++ saveTagsMap
        }
      }
    // 将数据保存到es
    tagsMapRDD.saveToEs(resource)
  }


  /**
    * 将DataFrame数据转换为JSON格式数据，存储到ElasticSearch索引Index
    */
  def saveJsonToEsIndex(dataFrame: DataFrame, resource: String): Unit = {

    // 获取SparkSession实例对象，导入隐式转换
    val spark: SparkSession = dataFrame.sparkSession
    import spark.implicits._

    // 导入spark和es集成的包
    import org.elasticsearch.spark._

    //  将用户标签DataFrame ->  DataSet  -> RDD  -> 将数据封装到Map集合中 -> RDD
    val esTagsDS: RDD[String] = dataFrame.as[UserTags].rdd
      .mapPartitions { iter =>
        iter.map { case UserTags(mainId, ids, tags) =>

          // a. 将ids和tags转化为Map集合
          val idsMap: Map[String, String] = TagUtils.idsStr2Map(ids)

          val tagsMap: Map[String, Double] = TagUtils.tagsStr2Map(tags)

          /**
            * b. 对tagsMap进行操作
            *    - 第一步 、(KW@丰田->1.0)   ->  KW -> 丰田:1.0
            *    - 第二步 、KW -> 丰田:1.0   ->  KW -> 丰田:1.0,二手车:1.0  -> 按照key进行分组,将相同key的value放在一个迭代器里面,将value拼接成字符串,使用逗号连接
            */
          val saveTagsMap: Map[String, String] = tagsMap
            .toList // 转化为list集合,相同的key不会被覆盖
            .map { case (tag, tagWeight) =>
            // 数据转换 : (KW@丰田->1.0)     ->  KW -> 丰田:1.0
            //  . 对 tag=KW@丰田 按照@ 分割
            val Array(tagName, tagValue) = tag.split("@")

            // b. 拼接需要的数据类型  ->  KW -> 丰田:1.0
            tagName -> s"$tagValue:$tagWeight"
          } //List[(String, String)]
            // c. 按照key分组
            .groupBy(_._1) //Map[String, List[(String, String)]]  -> 使用groupBy进行分组 -> 返回值是 Map[key,List(key,value)] value是原数据(k,v)的集合
            // d. 将相同key的value拼接成一个字符串
            .map { case (keyword, list) =>

            //val xx: Map[String, String] =list

            keyword -> list.map { case (key, value) => value }.mkString(",")
          }
          // 使用Json4s将Map集合转换为JSON格式数据
          compact(render(idsMap ++ saveTagsMap))
        }
      }
    // 保存到es
    esTagsDS.saveToEs(resource)
  }
}
