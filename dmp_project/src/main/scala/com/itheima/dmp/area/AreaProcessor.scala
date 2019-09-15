package com.itheima.dmp.area

import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.utils.HttpUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction

object AreaProcessor extends Processor {

  /**
    * 1. 获取geoHash 经纬度 数据
    * 2. 根据经纬度过滤出非大陆的数据
    * 3. 按照geoHash分组去重
    * 4. 获取组内经纬度的平均值
    * 5. 自定义UDF函数,传入平均经纬度,获取商圈信息
    *
    * @param dataframe
    * @return
    */
  override def processData(dataframe: DataFrame): DataFrame = {

    val spark = dataframe.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //自定义一个UDF函数,将工具类中的方法使用空格 + _ 转化为函数  方法名  _  --》 函数
    val get_area: UserDefinedFunction = udf(HttpUtils.loadGaode2Area _)

    val businessAreasDF: DataFrame = dataframe

      // 1. 获取geoHash 经纬度 数据
      .select($"geoHash", $"longitude", $"latitude")
      // 2. 根据经纬度过滤出非大陆的数据
      .filter($"longitude".geq(73.67).and($"longitude".leq(135.06)).and($"latitude".geq(3.87).and($"latitude".leq(53.56))))
      //3. 按照geoHash分组去重,获取组内经纬度的平均值
      .groupBy($"geoHash")
      //4. 获取组内经纬度的平均值,高德地图解析经纬度,要求6为小数
      .agg(
      round(avg($"longitude"), 6).as("avg_longitude"),
      round(avg($"latitude"), 6).as("avg_latitude")
    )
      //5. 自定义UDF函数,传入平均经纬度,获取商圈信息
      .select(
      $"geoHash".as("geoHash"),
      get_area($"longitude", $"latitude").as("area")
    )

    //6. 返回
    businessAreasDF
  }

}
