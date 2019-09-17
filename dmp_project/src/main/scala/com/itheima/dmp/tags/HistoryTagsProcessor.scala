package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.beans.{IdsWithTags, UserTags}
import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.utils.TagUtils
import org.apache.spark.sql.{DataFrame, Dataset}

object HistoryTagsProcessor extends Processor {

  // 标签衰减系数
  private val TAG_COEFFICIENT: Double = AppConfigHelper.TAG_COEFF

  override def processData(dataframe: DataFrame): DataFrame = {

    val spark = dataframe.sparkSession
    import spark.implicits._

    // 将dataFrame转化为DataSet处理
    val coeffTagsDS: Dataset[IdsWithTags] = dataframe.as[UserTags].mapPartitions { iter =>

      iter.map { case UserTags(main_id, ids, tags) =>

        // 1. 将字符串标签tags转化为map集合
        val tagsMap: Map[String, Double] = TagUtils.tagsStr2Map(tags)

        // 2. 对标签的权重进行操作 -> 乘以衰减因子
        val coeffTagsMap = tagsMap.map { case (tagKey, tagValue) => (tagKey, tagValue * TAG_COEFFICIENT) }

        // 3. 版本V1  ->  将coeffTagsMap转化为字符串,封装为样例类返回
        //UserTags(main_id, ids, TagUtils.map2Str(coeffTagsMap))

        // 版本V2     ->  封装Map便于用户统一识别使用数据
        IdsWithTags(main_id, TagUtils.idsStr2Map(ids), tagsMap)
      }
    }

    // 返回历史标签
    coeffTagsDS.toDF()
  }

}
