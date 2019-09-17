package com.itheima.dmp.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkEsRDDTest {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkContext实例对象
    val sc: SparkContext = {
      // a. 构建SparkConf对象，设置配置参数信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[4]")
      // b. 设置集成ES相关参数
      sparkConf.set("es.nodes", "node01,node02,node03")
      sparkConf.set("es.port", "9200")
      // 索引库不存在就创建
      sparkConf.set("es.index.auto.create", "true")
      // c. 创建SparkContext上下文实例对象
      val context: SparkContext =
        SparkContext.getOrCreate(sparkConf)
      // 返回
      context
    }


    // TODO: 数据类型为Map格式，保存ES中
    // a、模拟数据，数据类型为Map集合
    val gameMap = Map("media_type" -> "game", "title" -> "Final Fantasy VI", "year" -> "1994")
    val bookMap = Map("media_type" -> "book", "title" -> "Harry Potter", "year" -> "2010")
    val musicMap = Map("media_type" -> "music", "title" -> "Surfing With The Alien", "year" -> "1987")

    // b、导入隐式转换，保存数据至ES
    import org.elasticsearch.spark._
    val mapRDD: RDD[Map[String, String]] = sc.makeRDD(Seq(gameMap, bookMap, musicMap))
    // 保存的index和type
    mapRDD.saveToEs("spark-map/medias")

    // TODO: 数据类型为CaseClass格式，保存ES中
    // a、模拟数据，类型为Trip
    case class Trip(departure: String, arrival: String)
    val upcomingTrip = Trip("OTP", "SFO")
    val lastWeekTrip = Trip("MUC", "OTP")

    // 保存数据到ES
    import org.elasticsearch.spark.rdd.EsSpark
    val caseRdd: RDD[Trip] = sc.parallelize(Seq(upcomingTrip, lastWeekTrip))
    EsSpark.saveToEs(caseRdd, "spark-class/trips")

    // TODO: 数据类型为JSON格式数据，保存ES中
    val json1 =
      """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
    val jsonRDD: RDD[String] = sc.parallelize(Seq(json1, json2))
    jsonRDD.saveJsonToEs("spark-json/airports")

    val esRDD: RDD[(String, collection.Map[String, AnyRef])] = sc.esRDD("spark-json/airports")
    esRDD.foreach(println)


    sc.stop()
  }

}
