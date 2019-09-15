package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.beans.UserTags
import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.utils.TagUtils
import org.apache.spark.sql.{DataFrame, Dataset}

object HistoryTagsProcessor extends Processor {

  // 标签衰减系数
  private val TAG_COEFFICIENT: Double = AppConfigHelper.TAG_COEFF

  override def processData(dataframe: DataFrame): DataFrame = {

    val spark = dataframe.sparkSession
    import spark.implicits._

    //将dataframe转化为DataSet进行处理
    val coeffTagsDS: Dataset[UserTags] = dataframe.as[UserTags].mapPartitions { iter =>

      iter.map { case UserTags(mainId, idsStr, tagsStr) =>

        //将用户标签tagsStr转化为map集合
        val tagsMap: Map[String, Double] = TagUtils.tagsStr2Map(tagsStr)

        //修改权重
        val coeffTagsMap: Map[String, Double] = tagsMap.map {
          case (key, value) => (key, value * TAG_COEFFICIENT)
        }

        //返回
        UserTags(mainId, idsStr, TagUtils.map2Str(coeffTagsMap))
      }
    }

    // 返回历史标签DataFrame
    coeffTagsDS.toDF()
  }

}
