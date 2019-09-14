package com.itheima.dmp.utils

import ch.hsr.geohash.GeoHash
import com.itheima.dmp.beans.IpRegion
import com.itheima.dmp.config.AppConfigHelper
import com.maxmind.geoip.{Location, LookupService}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}


/// TODO: 解析ip工具类
object IPUtils {

  /**
    * 每条数据调用方法 都需要创建DbSearcher和LookupService实例对象 -> 可以优化成各个分区共用一个实例
    * @param ip
    * @return
    */
  def convertIp2Region(ip: String): IpRegion = {

    /// TODO: 1.将ip转换为省市
    val searcher = new DbSearcher(new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH)
    val region: String = searcher.btreeSearch(ip).getRegion
    val Array(_, _, province, city, _) = region.split("\\|")

    /// TODO:  2.将ip转化为经纬度
    val service = new LookupService(AppConfigHelper.IPS_DATA_GEO_PATH, LookupService.GEOIP_MEMORY_CACHE)
    val location: Location = service.getLocation(ip)
    val latitude: Double = location.latitude.toDouble
    val longitude: Double = location.longitude.toDouble

    /// TODO: 3.使用经纬度转换为GeoHash值
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 8)

    /// TODO: 4.返回样例对象
    IpRegion(ip, longitude, latitude, province, city, geoHash)
  }

  /**
    *重载方法:每个分区调用传入参数searcher,service -> 各个分区共用一个实例
    * @param ip
    * @param searcher
    * @param service
    * @return
    */
  def convertIp2Region(ip: String, searcher: DbSearcher, service: LookupService): IpRegion = {

    /// TODO: 1.将ip转换为省市
    val searcher: DbSearcher = new DbSearcher(new DbConfig(), AppConfigHelper.IPS_DATA_REGION_PATH)
    val region: String = searcher.btreeSearch(ip).getRegion
    val Array(_, _, province, city, _) = region.split("\\|")

    /// TODO:  2.将ip转化为经纬度
    val service: LookupService = new LookupService(AppConfigHelper.IPS_DATA_GEO_PATH, LookupService.GEOIP_MEMORY_CACHE)
    val location: Location = service.getLocation(ip)
    val latitude: Double = location.latitude.toDouble
    val longitude: Double = location.longitude.toDouble

    /// TODO: 3.使用经纬度转换为GeoHash值
    val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 8)

    /// TODO: 4.返回样例对象
    IpRegion(ip, longitude, latitude, province, city, geoHash)
  }

  /**
    * 测试工具类
    */
  def main(args: Array[String]): Unit = {

    val region: IpRegion = convertIp2Region("106.87.131.39")
    println(region)
  }
}
