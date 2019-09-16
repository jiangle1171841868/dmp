package com.itheima.dmp.test

import org.apache.spark.{SparkConf, SparkContext}

object SparkWithES {

  def main(args: Array[String]): Unit = {

    // 1、构建SparkContext实例对象
    val conf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[4]")
    conf.set("myes", "node01,node02,node03")
    conf.set("es.port", "9200")
    conf.set("es.index.auto.create", "true")

    val sc: SparkContext = new SparkContext(conf)

    // 2、模拟数据，要求数据类型为Map集合
    val numbers = Map("one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("OTP" -> "Otopeni", "SFO" -> "San Fran")

    // 3、保存数据到es
    import org.elasticsearch.spark._
    sc.parallelize(Seq(numbers,airports)).saveToEs("bigdata/docs")

    sc.stop()

  }

}
