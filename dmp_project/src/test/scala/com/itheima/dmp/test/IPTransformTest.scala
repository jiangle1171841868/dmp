package com.itheima.dmp.test

import ch.hsr.geohash.GeoHash
import com.maxmind.geoip.{Location, LookupService}
import org.lionsoul.ip2region.{DbConfig, DbSearcher}

/// TODO: 使用ip2region将ip转化为省份  使用GeoLite将ip转化为经纬度信息
object IPTransformTest {

  // 测试方法
  def main(args: Array[String]): Unit = {

    val ip: String = "36.62.163.115"

    ip2Region(ip)

    ip2Location(ip)
  }

  /**
    * IP -> Region（省份）、City（城市）
    *
    * @param ip
    */
  def ip2Region(ip: String): Unit = {

    /// TODO: 1.创建DbSearch实例
    /**
      * 参数:
      *    - bdConfig : DbConfig对象
      *    - dbFile   : ip2region.db文件路径
      * public DbSearcher( DbConfig dbConfig, String dbFile )
      */
    val dbFile: String = "datas/ip2region.db"
    val searcher = new DbSearcher(new DbConfig(), dbFile)

    /// TODO: 2.解析ip地址 获取region  中国|0|安徽省|滁州市|电信
    val region: String = searcher.btreeSearch(ip).getRegion
    println(region)

    /// TODO: 3.获取需要的省市信息
    val Array(_, _, province, city, _) = region.split("\\|")
    println(s"province=$province  city=$city")

  }

  /**
    * IP -> 经度和维度
    *
    * @param ip
    */
  def ip2Location(ip: String): Unit = {

    val databaseFile: String = "datas/GeoLiteCity.dat"

    /// TODO: 1.创建服务入口 转入字典路径和缓存级别
    val service = new LookupService(databaseFile, LookupService.GEOIP_MEMORY_CACHE)

    /// TODO: 2.搜索 获取数据  -> 根据ip查询  找方法的参数是ip的 -> 戳进去看看 是不是需要的方法
    val location: Location = service.getLocation(ip)

    /// TODO: 3.获取经纬度信息
    val longitude: Float = location.longitude
    val latitude: Float = location.latitude
    //经度 -> 117.28079 纬度 -> 31.863907 省份 -> 01  城市 -> Hefei
    //可以回去省份和城市 但是不准确 不支持中文
    println(s"经度 -> ${longitude} 纬度 -> ${latitude} 省份 -> ${location.region}  城市 -> ${location.city}")

    /// TODO: 4.转换经纬度为GeoHash值
    //参数:  纬度  经度  geohash的位数(越大越准确,最大12位)
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(location.latitude, location.longitude, 8)
    println(geoHash) //wtemkb9k

  }

  /**
    *
    *
    * @param location
    */

  def locationToGeoHash(location: Location) = {

    /**
      *
      * 快捷键   /o
      * match
      * {
      * case Some() =>
      * case None =>
      * }
      */

  }
}
