package com.itheima.dmp.utils

import java.time.Duration

import com.itheima.dmp.config.AppConfigHelper
import okhttp3.{OkHttpClient, Request, Response}
import org.json4s
import org.json4s.JValue
import org.json4s.jackson.JsonMethods.parse

object HttpUtils {

  /**
    * 根据经纬度信息,请求高德,获取商圈信息 -> json格式
    *
    * @param longitude
    * @param latitude
    * @return ->  返回结果可能不存在,使用Option[String]
    */
  def getLocationInfo(longitude: Double, latitude: Double): Option[String] = {

    // 1. 拼接url 基本url + key + 经纬度信息
    val url: String = AppConfigHelper.AMAP_URL + s"$longitude,$latitude"

    try {
      // 2. 创建OkHttpClient实例对象
      val httpClient = new OkHttpClient.Builder()
        .connectTimeout(Duration.ofSeconds(60L)) //设置连接超时时间
        .build()

      // 3. 创建请求对象,构造者模式构建,是java中的类  -> 需要 new Request.Builder()
      val request: Request = new Request.Builder()
        .url(url)   //请求url
        .get()     //请求方式
        .build()

      // 4. 发送请求,获取结果
      val response: Response = httpClient.newCall(request).execute()

      // 5. 打印结果
      //a.判断是否请求成功 ->   状态码范围  ->  code >= 200 && code < 300
      if (response.isSuccessful) {
        //b.获取响应体中的数据
        val result: String = response.body().string()

        //c.获取成功.返回json数据
        Some(result)
      } else {
        //d. 请求不成功,返回None
        None
      }
    } catch {
      case e: Exception => e.printStackTrace(); None
    }
  }

  /**
    * 解析高德地图返回的JSON格式数据,返回格式为 商圈1:商圈2:商圈3
    *
    * @param gaodeJson
    * @return
    */
  def parseJson(gaodeJson: String): String = {

    // 1. 解析json数据获取json对象
    //使用前需要导包 ->  import org.json4s._  import org.json4s.jackson.JsonMethods._
    val jValue: JValue = parse(gaodeJson)

    // 2. 获取商圈信息,商圈信息是一个数据,递归获取子节点
    val areaJValue: JValue = jValue.\\("businessAreas")

    // 3. 获取商圈信息(数组)里面的子节点
    val childrens: List[json4s.JValue] = areaJValue.children

    var businessArea = ""

    // 4. 遍历数组,获取商圈信息  ->  name
    for (children <- childrens) {

      //获取商圈的名称,进行拼接 -> name1:name2:name3
      businessArea += s"${children.\("name").values}:"
    }

    //去掉拼接最后多余的 : -> 返回
    businessArea.stripSuffix(":")

  }

  /**
    * 根据经纬度请求高德,返回数据,解析出商圈信息
    *
    * @param longitude
    * @param latitude
    * @return
    */
  def loadGaode2Area(longitude: Double, latitude: Double): String = {

    // 1. 发送请求,获取数据
    val jsonOption: Option[String] = getLocationInfo(longitude, latitude)

    // 2. 有数据就解析
    jsonOption match {
      case Some(jsonStr) => parseJson(jsonStr)
      case None => ""
    }
  }

  //测试工具类
  def main(args: Array[String]): Unit = {

    println(loadGaode2Area(116.310003, 39.991957))
  }
}
