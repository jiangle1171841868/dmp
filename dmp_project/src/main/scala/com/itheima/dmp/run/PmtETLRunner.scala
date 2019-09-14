package com.itheima.dmp.run

import com.itheima.dmp.config.{AppConfigHelper, SparkConfigHelper}
import com.itheima.dmp.etl.IPProcessor
import com.itheima.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/// TODO: 获取json数据进行ETL处理,添加省市、经纬度信息,删除重复字段,保存到kudu中
object PmtETLRunner {

  def main(args: Array[String]): Unit = {

    /// TODO: 1.构架sparkSession实例对象
    val spark: SparkSession = SparkSessionUtils.spark

    /// TODO: 2.读取json格式的数据
    val dataFrame: DataFrame = spark.read.json(AppConfigHelper.AD_DATA_PATH)

    /// TODO: 3.调用ETL处理方法
    IPProcessor.processData(dataFrame)

    /// TODO: 4.保存到kudu表
    import com.itheima.dmp.utils.KuduUtils._
    val tableName: String = AppConfigHelper.AD_MAIN_TABLE_NAME
    val schema: StructType = dataFrame.schema
    val keys: Seq[String] = Seq("uuid")
    //a.创建表
    spark.createKuduTable(tableName, schema, keys)

    //b.保存数据
    dataFrame.saveAsKuduTable(tableName)

    /// TODO: 5.应用完成,关闭资源
    spark.stop()
  }

}
