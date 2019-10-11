package com.itheima.dmp.test


import org.json4s
import org.json4s.JValue
import org.json4s.jackson.JsonMethods._


/**
  * 解析高德地图返回的JSON,返回格式为 商圈1:商圈2:商圈3
  *    - 1.使用json4s解析json数据
  *    - 2.获取商圈信息
  *    - 3.将商圈信息拼接为需要的格式
  */

object ParseJson {

  def main(args: Array[String]): Unit = {


    val regeoJson: String =
      """{
          "status": "1",
          "regeocode": {
              "addressComponent": {
                  "city": [],
                  "province": "北京市",
                  "adcode": "110108",
                  "district": "海淀区",
                  "towncode": "110108015000",
                  "streetNumber": {
                      "number": "5号",
                      "location": "116.310454,39.9927339",
                      "direction": "东北",
                      "distance": "94.5489",
                      "street": "颐和园路"
                  },
                  "country": "中国",
                  "township": "燕园街道",
                  "businessAreas": [
                      {
                          "location": "116.303364,39.97641",
                          "name": "万泉河",
                          "id": "110108"
                      },
                      {
                          "location": "116.314222,39.98249",
                          "name": "中关村",
                          "id": "110108"
                      },
                      {
                          "location": "116.294214,39.99685",
                          "name": "西苑",
                          "id": "110108"
                      }
                  ],
                  "building": {
                      "name": "北京大学",
                      "type": "科教文化服务;学校;高等院校"
                  },
                  "neighborhood": {
                      "name": "北京大学",
                      "type": "科教文化服务;学校;高等院校"
                  },
                  "citycode": "010"
              },
              "formatted_address": "北京市海淀区燕园街道北京大学"
          },
          "info": "OK",
          "infocode": "10000"
      }"""


    // 1. 解析json数据获取json对象
    //使用前需要导包 ->  import org.json4s._  import org.json4s.jackson.JsonMethods._
    val jValue: JValue = parse(regeoJson)

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

    //去掉拼接最后多余的 :
    businessArea = businessArea.stripSuffix(":")

    println(businessArea)

  }


}
