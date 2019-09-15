package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.TagsMaker
import org.apache.spark.sql.Row

/**
  * 设备类型标签的生成
  */
object Tags4DeviceMaker extends TagsMaker {


  override def make(row: Row, dic: Map[String, String]): Map[String, Double] = {

    // 1. 操作系统标签：client 设备类型 （1：android 2：ios 3：wp）如果获取不到就是4类型，4就是其他的
    val client = row.getAs[Long]("client").toString
    val clientId = dic.getOrElse(client, "D00010004")

    // 2. 联网方式标签：networkmannername 联网方式名称，如果没有就给NETWORKOTHER代表 其他
    val networkName = row.getAs[String]("networkmannername")
    val networkId = dic.getOrElse(networkName, "D00020005")

    // 3. 第三、运营商的标签
    val ispName = row.getAs[String]("ispname")
    val ipsId = dic.getOrElse(ispName, "D00030004")

    // 4. 返回
    Map(s"DC@$clientId" -> 1.0, s"DN@$networkId" -> 1.0, s"DI@$ipsId" -> 1.0)

  }
}
