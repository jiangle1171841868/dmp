package com.itheima.dmp.area

import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.utils.HttpUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  def processDataV1(dataframe: DataFrame): DataFrame = {

    val spark = dataframe.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    //自定义一个UDF函数,将工具类中的方法使用空格 + _ 转化为函数  方法名  _  --》  函数
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
      get_area($"avg_longitude", $"avg_latitude").as("area")
    )

    //6. 返回
    businessAreasDF
  }

  /**
    * 1. 获取geoHash 经纬度 数据
    * 2. 根据经纬度过滤出非大陆的数据
    * 3. 按照geoHash分组去重
    * 4. 获取组内经纬度的平均值
    * 5. 获取已有的商圈表
    * 6. 与ODS表关联（左外连接），获取新的经纬度信息
    * 7. 自定义UDF函数,传入平均经纬度,获取商圈信息
    *
    * @param dataframe
    * @return
    */
  override def processData(dataframe: DataFrame): DataFrame = {

    val spark = dataframe.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._
    import com.itheima.dmp.utils.KuduUtils._


    val geoHashDF: DataFrame = dataframe

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

    //打印过滤前后的数据量
    logWarning(s"从Kudu读取ODS表数据量：${dataframe.count()}")
    logWarning(s"对ODS表数据经过过滤去重后数据量：${geoHashDF.count()}")

    // 5. 获取已有的商圈表
    val areaDFOption: Option[DataFrame] = spark.readKuduTable(AppConfigHelper.BUSINESS_AREAS_TABLE_NAME)

    // 6. 与ODS表关联（左外连接），获取新的经纬度信息
    val geoHashWithAreaDF: DataFrame = areaDFOption match {
      //历史表有数据,关联新表,去掉存在的geoHash,避免重复查询数据
      case Some(areaDFOption) =>
        geoHashDF.join(
          areaDFOption, //关联的表
          geoHashDF.col("geoHash") === areaDFOption.col("geo_hash"), //关联的字段  -> 调用and方法可以多个关联字段
          "left" //关联方式
        )
          // 新数据里面没有area字段,过滤出area为null的就是新的数据,使用新的数据去调用高德 API
          .filter($"area".isNull)
      //历史表没有数据,直接返回新表
      case None => geoHashDF
    }
    logWarning(s"去重已存在商圈信息表的数据后数据量: ${geoHashWithAreaDF.count()} ...............")

    //6. 自定义UDF函数,传入平均经纬度,获取商圈信息
    //自定义一个UDF函数,将工具类中的方法使用空格 + _ 转化为函数  方法名  _  --》  函数
    val get_area: UserDefinedFunction = udf(HttpUtils.loadGaode2Area _)

    val areaDF: DataFrame = geoHashWithAreaDF.select(
      $"geoHash".as("geo_hash"),
      get_area($"avg_longitude", $"avg_latitude").as("area")
    )

    //6. 返回
    areaDF
  }

  /**
    * 使用SQL语句,生成商圈信息表
    *
    * @param dataframe
    * @return
    */
  def processDataV2(dataframe: DataFrame): DataFrame = {

    val BUSINESS_AREAS_TABLE_NAME = AppConfigHelper.BUSINESS_AREAS_TABLE_NAME
    val spark: SparkSession = dataframe.sparkSession

    import com.itheima.dmp.utils.KuduUtils._

    // 1. 将表注册为视图
    // a. 将ods表注册为临时视图
    dataframe.createOrReplaceTempView("viea_ods")
    // b. 将area表注册为临时视图
    val areaDF: DataFrame = spark.readKuduTable(BUSINESS_AREAS_TABLE_NAME).getOrElse(dataframe)

    areaDF.createOrReplaceTempView("view_area")

    // 2. 自定义UDF函数
    spark.udf.register(
      "get_area", //函数名
      HttpUtils.loadGaode2Area _ //函数   -> 方法加下划线转化为函数
    )

    // 3. 执行SQL查询
    // a. 获取SQL语句
    val areaSQL: String = AreaSQLConstant.areaSQL("viea_ods", "view_area")
    // b. 执行查询
    spark.sql(areaSQL)

  }

}
