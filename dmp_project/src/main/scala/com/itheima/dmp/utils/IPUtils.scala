package com.itheima.dmp.utils

import ch.hsr.geohash.GeoHash
import com.itheima.dmp.config.AppConfigHelper
import com.maxmind.geoip.{Location, LookupService}
import org.lionsoul.ip2region.{DataBlock, DbConfig, DbSearcher}

//封装ip解析后的省市、经纬度信息
case class IpRegion(ip: String, longitude: Double, latitude: Double,
                    province: String, city: String, geoHash: String)

/// TODO: 解析ip工具类
object IPUtils {

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
}
