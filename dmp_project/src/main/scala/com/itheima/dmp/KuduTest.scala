package com.itheima.dmp

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.codehaus.jackson.util.BufferRecycler.ByteBufferType

object KuduTest {

  def main(args: Array[String]): Unit = {

    /// TODO:  构建sparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .getOrCreate()

    /// TODO: 2.构架kudu表 表名  schema keys tableOption
    //a.表名
    val tableName = "kudu_students"

    //b.schema
    val schema = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true)
      )
    )

    //c.keys
    val keys: Seq[String] = Seq("id")

    import spark.implicits._

    /// TODO: 3.调用隐式转化方法 操作kudu表
    // TODO:  a.实现：SparkSession.createKuduTable(
    import com.itheima.dmp.utils.KuduUtils._
    spark.createKuduTable(tableName, schema, keys, isDelete = false)

    // TODO: b.实现：SparkSession.deleteKuduTable()
    //spark.deleteKuduTable(tableName)

    //  TODO: c.实现：DataFrame.saveAsKuduTable
    val studentsDF: DataFrame = Seq(
      (10001, "zhangsan", 23), (10002, "lisi", 22), (10003, "wagnwu", 23),
      (10004, "xiaohong", 21), (10005, "tainqi", 235), (10006, "zhaoliu", 24)
    ).toDF("id", "name", "age")
    studentsDF.saveAsKuduTable(tableName)

    //  TODO: d.实现：SparkSession.readKuduTable(tableName) 返回的数据是Option[DataFrame]  需要模式匹配获取数据
    spark.readKuduTable(tableName) match {
      case Some(dataFrame) => dataFrame.show(10, false)
      case None => println(s"你查询的表 $tableName 不存在....")
    }

    /// TODO: 关闭资源
    spark.stop()
  }

}
