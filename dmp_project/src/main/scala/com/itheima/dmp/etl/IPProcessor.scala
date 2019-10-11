package com.itheima.dmp.etl


import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.beans.IPRegion
import com.itheima.dmp.config.AppConfigHelper
import com.itheima.dmp.utils.IPUtils
import com.maxmind.geoip.LookupService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/**
  * 1.将dataframe转化为RDD[Row]进行处理 (dataframe内部为row,调用mapPartition 会出现问题)
  * 2.对每个分区数据进行操作
  *       - a.获取ip
  *       - b.调用工具类解析ip 获取省市 经纬度和geoHash   (可以优化 工具类中对象的创建)
  *       - c.将省市 经纬度和geoHash 添加到每行数据的尾部(row.toSeq)
  *       - d.返回新的row对象
  * 3.在原来schema基础上添加  新添加字段的schema   ->  创建新的DataFrame
  * 4.删除数据中的重复字段
  *    - select 选择需要的字段 过滤出重复字段
  *    - drop   直接根据字段名删除重复的字段
  */
object IPProcessor extends Processor {

  override def processData(dataframe: DataFrame): DataFrame = {

    val spark = dataframe.sparkSession

    /// TODO: 1. 将dataframe转化为RDD[Row]进行处理
    val newRDD: RDD[Row] = dataframe.rdd.mapPartitions { rows =>

      //创建DbSearcher实例对象
      val searcher: DbSearcher = new DbSearcher(new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH)
      //创建LookupService实例对象
      val service: LookupService = new LookupService(AppConfigHelper.IPS_DATA_GEO_PATH, LookupService.GEOIP_MEMORY_CACHE)

      /// TODO: 2. 对每个分区数据(Row)进行操作,获取ip,调用工具类解析ip 获取省市 经纬度和geoHash
      rows.map { row =>

        // a. 获取ip
        val ip = row.getAs[String]("ip")

        // b. 解析ip
        val region: IPRegion = IPUtils.convertIp2Region(ip, searcher, service)

        // c. 将省市 经纬度和geoHash 添加到每行数据的尾部  -> 添加在后面 +:  添加在后面 :+ 添加的元素放在 + 那一边
        val newSeq: Seq[Any] = row.toSeq :+
          region.province :+
          region.city :+
          region.latitude :+
          region.longitude :+
          region.geoHash

        // d. 返回新的row
        Row.fromSeq(newSeq)
      }
    }

    /// TODO: 3. 在原来schema基础上添加  新添加字段的schema   ->  创建新的DataFrame
    //a..在原来schema基础上添加  新添加字段的schema
    val newSchema = dataframe.schema
      .add("province", StringType, true)
      .add("city", StringType, nullable = true)
      .add("latitude", DoubleType, nullable = true)
      .add("longitude", DoubleType, nullable = true)
      .add("geoHash", StringType, nullable = true)

    //b.创建新的schema
    val newDF: DataFrame = spark.createDataFrame(newRDD, newSchema)

    /// TODO: 4.删除数据中的重复字段

    //a.方式一:select选择DataFrame中需要的字段 -> 过滤出重复字段
    import spark.implicits._
    //$ 和 ` 都是spark.implicits里面的函数 -> 将字符串转化为列Column对象
    val selectColumns: Seq[Column] = Seq(
      'sessionid, 'advertisersid, 'adorderid, 'adcreativeid,
      'adplatformproviderid, 'sdkversion, 'adplatformkey,
      'putinmodeltype, 'requestmode, 'adprice, 'adppprice,
      'requestdate, 'ip, 'appid, 'appname, 'uuid, 'device,
      'client, 'osversion, 'density, 'pw, 'ph, 'longitude,
      'latitude, 'province, 'city, 'ispid, 'ispname, 'networkmannerid,
      'networkmannername, 'iseffective, 'isbilling, 'adspacetype,
      'adspacetypename, 'devicetype, 'processnode, 'apptype,
      'district, 'paymode, 'isbid, 'bidprice, 'winprice,
      'iswin, 'cur, 'rate, 'cnywinprice, 'imei, 'mac, 'idfa,
      'openudid, 'androidid, 'rtbprovince, 'rtbcity, 'rtbdistrict,
      'rtbstreet, 'storeurl, 'realip, 'isqualityapp, 'bidfloor, 'aw,
      'ah, 'imeimd5, 'macmd5, 'idfamd5, 'openudidmd5, 'androididmd5,
      'imeisha1, 'macsha1, 'idfasha1, 'openudidsha1, 'androididsha1,
      'uuidunknow, 'userid, 'reqdate, 'reqhour, 'iptype, 'initbidprice,
      'adpayment, 'agentrate, 'lomarkrate, 'adxrate, 'title, 'keywords,
      'tagid, 'callbackdate, 'channelid, 'mediatype, 'email, 'tel,
      'age, 'sex, 'geoHash
    )

    //val kuduDF = newDF.select(selectColumns: _*) //    _*    ->  将集合转换为一个个元素

    //b.方式二:drop:参数 字段名   -> 直接根据字段名删除重复的字段
    val kuduDF: DataFrame = newDF.drop("provincename", "cityname", "lang", "lat")

    //返回df
    kuduDF
  }
}