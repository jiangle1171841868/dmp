package com.itheima.dmp.test

import java.time.Duration

import okhttp3.{OkHttpClient, Request, Response}


//拼接key和经纬度url,发送http请求到高德,获取商圈信息(接送数据)
object OkHttpTest {

  def main(args: Array[String]): Unit = {

    // 1. 拼接url 基本url + key + 经纬度信息
    val GAODE_KEY = "98a87915dd1032f8ed4c2cb8df5176ee"
    val url = s"https://restapi.amap.com/v3/geocode/regeo?" + s"location=116.310003,39.991957&key=$GAODE_KEY"

    // 2. 创建OkHttpClient实例对象
    val httpClient = new OkHttpClient.Builder()
      .connectTimeout(Duration.ofSeconds(60L)) //设置连接超时时间
      .build()

    // 3. 创建请求对象,构造者模式构建,是java中的类  -> 需要 new Request.Builder()
    val request: Request = new Request.Builder()
      .url(url)     //请求url
      .get()        //请求方式
      .build()

    // 4. 发送请求,获取结果
    val response: Response = httpClient.newCall(request).execute()

    // 5. 打印结果
    //a.判断是否请求成功 ->   状态码范围  ->  code >= 200 && code < 300
    if (response.isSuccessful) {

      //b.获取响应体中的数据
      val result: String = response.body().string()

      println(result)
    }

  }

}
