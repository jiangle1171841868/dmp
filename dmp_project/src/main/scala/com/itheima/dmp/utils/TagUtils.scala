package com.itheima.dmp.utils

import scala.collection.mutable

/**
  * Map集合 <-> 字符串  相互转化
  */
object TagUtils {

  /**
    * Map类型的数据转换为字符串，格式为：(k1->v1),(k2->v2),(k3->v3)
    *
    * @param kvMap Map 集合
    * @return
    */
  def map2Str(kvMap: Map[String, _]): String = {

    kvMap
      .toList.sortBy(kv => kv._1) //按照key进行排序
      .map { case (key, value) => s"($key=>$value)" } //转化为字符串   ->  (k1->v1)
      .mkString(",") //使用逗号连接集合中的元素   ->  (k1->v1),(k2->v2),(k3->v3)

  }

  /**
    * 从Kudu中获取到的标签信息是字符串类型转换为Map集合
    * 原数据  ->  (k1->v1),(k2->v2),(k3->v3)
    * @param tagsStr 标签数据字符串
    * @return
    */
    def tagsStr2Map(tagsStr: String): Map[String, Double] = {

    var map: mutable.Map[String, Double] = mutable.Map[String, Double]()

    // a. 按照逗号切分成数组 -> (k1->v1)
    val tags: Array[String] = tagsStr.trim.split("\\,")

    // b. 遍历集合,将数组元素转化为k v存储到map结合中
    for (tag <- tags) {

      val Array(key, value) = tag.stripPrefix("(") //脱去前面的括号
        .stripSuffix(")")//脱去后面的括号
        .split("->") //按照-> 切割出k v

      // c. 将数据数据保存到map集合
      map += key -> value.toDouble
    }

    // d. 将集合转化为不可变集合,返回  ->   返回值直接使用Map  默认就是不可变的
    map.toMap
  }


  /**
    * 从Kudu中获取到的id集合是字符串类型转换为Map
    *
    * @param idsStr 用户标识IDs字符串
    * @return
    */
  def idsStr2Map(idsStr: String): Map[String, String] = {

    // 数据格式：(imei->214052893329662),(mac->52:54:00:50:12:35)

    var map: mutable.Map[String, String] = mutable.Map[String, String]()

    // a. 按照逗号切分成数组 -> (imei->214052893329662)
    val ids: Array[String] = idsStr.trim.split(",")

    // b. 遍历集合,将数组元素转化为k v存储到map结合中
    for (id <- ids) {

      val Array(key, value) = id.stripPrefix("(") //脱去前面的括号
        .stripSuffix(")") //脱去后面的括号
        .split("->") //按照-> 切割出k v

      // c. 将数据数据保存到map集合
      map += key -> value
    }

    // d. 将集合转化为不可变集合,返回
    map.toMap
  }

  /**
    * 工具类测试
    */

  def main(args: Array[String]): Unit = {

    val str="(k1->20),(k2->10),(k3->30)"

    //idsStr2Map(str).foreach(println)

    tagsStr2Map(str).foreach(println)
  }
}
