package com.itheima.kudu.spark.sparksqlkudu

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQLKuduRead {

  def main(args: Array[String]): Unit = {

    // TODO: 1、构建SparkSession实例对象
    val spark: SparkSession = {

      //a.构建配置对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")

      //b.构建sparksession对象
      val session: SparkSession = SparkSession.builder()
        .config(sparkConf).getOrCreate()

      session.sparkContext.setLogLevel("WARN")

      session
    }

    /// TODO: 2.读取kudu表的数据
    import org.apache.kudu.spark.kudu._
    var kuduDF: DataFrame = spark.read
      //设置kudu参数
      .option("kudu.master", "node01:7051,node02:7051,node03:7051")
      .option("kudu.table", "kudu_itcast_users")
      .kudu

    kuduDF.printSchema()
    kuduDF.show(10, false)

    /// TODO: 关闭资源
    spark.stop()

  }

}
