package com.itheima.kudu.spark.ddl

import java.util

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object CreateTableTest {

  def main(args: Array[String]): Unit = {

    // TODO: 1、构建SparkSession实例对象
    var (sc, spark) = {

      val sparkConf = new SparkConf()
        .setMaster("local[4]").setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      // 采用建造者模式创建SparkSession实例
      val spark: SparkSession = SparkSession.builder()
        .config(sparkConf) // 设置应用配置信息
        .getOrCreate()

      spark.sparkContext.setLogLevel("WARN")

      (spark.sparkContext, spark)
    }

    /// TODO: 2.构架kuduContext实例对象
    val kuduMaster = "node01:7051,node02:7051,node03:7051"
    val context = new KuduContext(kuduMaster, sc)

    /// TODO: 3.调用方法 创建表
    createKuduTable(context, "kudu_itcast_users")

    /// TODO: 4.关闭资源
    sc.stop()
  }

  /**
    * 创建表的方法
    * 1.表名
    * 2.schema
    * 3.主键
    * 4.分区副本
    *
    * @param context
    * @param tableName
    */
  def createKuduTable(context: KuduContext, tableName: String): Unit = {

    /// TODO: 1.构建表的schema
    val schema = StructType(
      Array(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true),
        StructField("gender", StringType, true)
      )
    )

    /// TODO: 2.设置主键 是一个序列 可以多个
    val keys: Seq[String] = Seq("id")

    /// TODO: 3.设置分区和副本
    val tableOptions = new CreateTableOptions

    //a.使用colums集合封装 分区的字段
    val colums: util.ArrayList[String] = new util.ArrayList[String]()
    colums.add("id")

    /**
      * 设置hash分区 还是java的类
      * 参数:List< String> columns, int buckets
      */

    tableOptions.addHashPartitions(colums, 3)
    tableOptions.setNumReplicas(3)

    /// TODO: 4.使用kudu上下文对象创建表
    context.createTable(tableName, schema, keys, tableOptions)

  }

}
