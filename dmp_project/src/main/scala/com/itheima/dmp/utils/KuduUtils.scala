package com.itheima.dmp.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

class KuduUtils extends Logging {

  // 加载Kudu配置文件
  val config: Config = ConfigFactory.load("kudu.conf")

  //创建kudu表需要参数
  var spark: SparkSession = _
  var context: KuduContext = _

  //kudu表添加数据
  var dataFrame: DataFrame = _

  //辅助构造器 初始化SparkSession
  def this(spark: SparkSession) = {
    //辅助构造器第一行需要调用 主构造器 或者其他存在的辅助构造器
    this()
    this.spark = spark

    //初始化KuduContext
    val context = new KuduContext(config.getString("kudu.master"), spark.sparkContext)
    this.context = context
  }

  //辅助构造器 初始化DataFrame
  def this(dataFrame: DataFrame) = {
    //调用上一个辅助构造器 传入sparkSession实例对象
    this(dataFrame.sparkSession)
    this.dataFrame = dataFrame
  }


  /**
    * 创建表
    *
    * @param tableName
    * @param schema
    * @param keys
    * @param isDelete 表存在是否允许删除
    */
  def createKuduTable(tableName: String, schema: StructType, keys: Seq[String], isDelete: Boolean = true):  Unit = {

    //1.判断表是否存在  存在判断是否可以删除
    if (context.tableExists(tableName)) {
      if (isDelete) {
        //可以删除
        context.deleteTable(tableName)
        logInfo(s"表 $tableName 在kudu中已经存在,已经删除....")
      } else {
        logInfo(s"表 $tableName 在kudu中已经存在,不允许删除....")
        return
      }
    }

      //2.表不存在开始创建
      //a.设置表的分区  默认按照主键进行hash分区
      val options = new CreateTableOptions

      //将scala中封装主键的集合seq 转化为java的集合 作为分区的主键
      import scala.collection.JavaConverters._
      options.addHashPartitions(keys.asJava, 3)

      //b.设置副本数
      options.setNumReplicas(3)

      //c.创建表
      context.createTable(tableName, schema, keys, options)

      //d.打印日志
      logInfo(s"kudu表 $tableName 创建成功.......")

  }

  /**
    * 删除表
    *
    * @param tableName
    */
  def deleteKuduTable(tableName: String) = {

    if (context.tableExists(tableName)) {

      //a.删除表
      context.deleteTable(tableName)

      //b.打印日志
      logInfo(s"kudu表 $tableName 已经删除....")

    }
  }

  /**
    * 保存数据到kudu表
    *
    * @param tableName
    */
  def saveAsKuduTable(tableName: String) = {

    import org.apache.kudu.spark.kudu.KuduDataFrameWriter
    //a.判断dataframe是否能为空
    if (dataFrame == null) {
      logInfo("DataFrame为空,重新构造新的非空DataFrame来添加数据...")
    } else (

      //b.插入数据
      dataFrame.write
        .mode(SaveMode.Append)
        // 设置Kudu Master地址
        .option("kudu.master", config.getString("kudu.master"))
        // 设置读取Kudu表的名称
        .option("kudu.table", tableName)
        .kudu
      )
  }

  /**
    * 查询数据 -> 查询的表可能不存在 所以设置返回值类型是Option[DataFrame]
    *
    * @param tableName
    * @return
    */
  def readKuduTable(tableName: String): Option[DataFrame] = {

    //判断表是否存在
    if (context.tableExists(tableName)) {

      //a.表存在 获取数据
      import org.apache.kudu.spark.kudu.KuduDataFrameReader
      val kuduDF: DataFrame = spark.read
        // 设置Kudu Master地址
        .option("kudu.master", config.getString("kudu.master"))
        // 设置读取Kudu表的名称
        .option("kudu.table", tableName)
        .kudu

      logInfo(s"你查询的 ${tableName} 数据已经获取到....")

      //返回查询的数据
      Some(kuduDF)
    } else {
      //打印日志
      logInfo(s"你查询的表 $tableName 不存在....")
      //b.表不存在 返回none
      None

    }
  }
}


object KuduUtils {

  //1.隐式转换函数 SparkSession ->  KuduUtils
  implicit def SparkSessionToKuduUtils(spark: SparkSession): KuduUtils = {
    new KuduUtils(spark)
  }


  //2.隐式转化 DataFrame ->  KuduUtils
  implicit def DataFrameToKuduUtils(dataFrame: DataFrame): KuduUtils = {
    new KuduUtils(dataFrame)
  }


}