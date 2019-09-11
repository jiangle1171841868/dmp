package com.itheima.kudu.spark.sparksqlkudu

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSQLKuduWrite {

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


    // TODO: 2.读取kudu表的数据
    import org.apache.kudu.spark.kudu._
    var kuduDF: DataFrame = spark.read
      //设置kudu参数
      .option("kudu.master", "node01:7051,node02:7051,node03:7051")
      .option("kudu.table", "kudu_itcast_users")
      .kudu

    kuduDF.printSchema()
    kuduDF.show(10, false)

    import spark.implicits._
    import org.apache.spark.sql.functions._

    /// TODO: 数据ETL处理
    //  需求:  age+1   判断性别  男设置为M  女设置为F


    //自定义udf函数
    spark.udf.register(
      "age_add",
      (age: Int) => age + 1
    )

    udf((gender:String)=>if(gender.trim.equals("男"))  "M" else "F")

    /// TODO: 3.使用DSL分析
    val userDF: DataFrame = kuduDF
      //选择查询的字段
      .select(
      ($"id" + 1).as("id"),
      $"name",
      ($"age" + 1).as("age"),
      when($"gender".equalTo("男"), "M").otherwise("F").as("gender")
    )
    userDF.printSchema()
    userDF.show(10, true)

    /// TODO: 4.注册为临时视图 使用sql分析
    userDF.createOrReplaceTempView("user")

    val sqlDF = spark.sql(
      """
        |
        |select
        |  id,name,(age + 1) as age ,
        |case
        |  when trim(gender)="男" then "M" else "F" end as gender
        |from
        |  user
        |
      """.stripMargin)

    /// TODO: 5.将数据保存到kudo表中
    userDF
      //输出考虑降低分区数
      .coalesce(1)
      .write
      //append 就相当于 upsert 不支持其他的  only Append is supported
      .mode(SaveMode.Append)
      //设置kudu参数
      .option("kudu.master", "node01:7051,node02:7051,node03:7051")
      .option("kudu.table", "kudu_itcast_users")
      .kudu

    /// TODO: 关闭资源
    spark.stop()

  }
}
