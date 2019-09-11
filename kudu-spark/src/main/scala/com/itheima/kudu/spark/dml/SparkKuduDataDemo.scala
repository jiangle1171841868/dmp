package com.itheima.kudu.spark.dml

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkKuduDataDemo {


  def insertData(spark: SparkSession, kuduContext: KuduContext, tableName: String) = {

    //1.准备数据 封装成DF
    val usersDF: DataFrame = spark.createDataFrame(
      Seq(
        (1001, "zhangsan", 23, "男"),
        (1002, "lisi", 22, "男"),
        (1003, "xiaohong", 24, "女"),
        (1005, "zhaoliu2", 33, "男")
      )
    ).toDF("id", "name", "age", "gender")

    //2.使用kuduContext执行插入  DF里面是row  将DataFrame的行(row)插入Kudu表
    ///方式一:insertRows 如果行已经存在,会报错  不能插入
    //kuduContext.insertRows(usersDF, tableName)

    //方式二:行存在就忽略
    kuduContext.insertIgnoreRows(usersDF, tableName)
  }

  def selectData(spark: SparkSession, kuduContext: KuduContext, tableName: String) = {

    //1.设置查询的列
    val colums: Seq[String] = Seq("id", "name", "age")

    //2.使用kuduContext执行查询
    val dataRDD: RDD[Row] = kuduContext.kuduRDD(spark.sparkContext, tableName, colums)
    import spark.implicits._

    //3.打印数据
    dataRDD.foreachPartition { iter =>
      iter.foreach { row =>
        println(s"p-${TaskContext.getPartitionId()}: id = ${row.getInt (0)}" + s", name = ${row.getString(1)}, age = ${row.getInt (2)}")
      }
    }

  }

  //数据不存在就插入 存在就更新
  def upsertData(spark: SparkSession, kuduContext: KuduContext, tableName: String) = {

    val usersDF: DataFrame = spark.createDataFrame(
      Seq(
        (1001, "zhangsa风", 24, "男"),
        (1006, "tianqi", 33, "男")
      )
    ).toDF("id", "name", "age", "gender")

    kuduContext.upsertRows(usersDF,tableName)

  }

  def main(args: Array[String]): Unit = {

    // TODO: 1、构建SparkSession实例对象
    var spark = {

      val sparkConf = new SparkConf()
        .setMaster("local[4]").setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      // 采用建造者模式创建SparkSession实例
      val spark: SparkSession = SparkSession.builder()
        .config(sparkConf) // 设置应用配置信息
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      spark
    }

    /// TODO: 2.构建kuduContext实例对象
    val kuduMaster = "node01:7051,node02:7051,node03:7051"
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

    /// TODO: 3.增删改 操作
    val tableName = "kudu_itcast_users"

    // 插入数据
    //insertData(spark, kuduContext, tableName)
    // 查询数据
    //selectData(spark, kuduContext, tableName)
    // 更新数据
    //updateData(spark, kuduContext, tableName)
    // 插入更新数据  数据不存在就插入 存在就更新
    upsertData(spark, kuduContext, tableName)
    // 删除数据
    //deleteData(spark, kuduContext, tableName)


    // TODO：4、应用结束，关闭资源
    spark.stop()


  }
}
