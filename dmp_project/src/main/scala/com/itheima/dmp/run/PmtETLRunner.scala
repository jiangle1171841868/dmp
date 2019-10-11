package com.itheima.dmp.run

import com.itheima.dmp.config.{AppConfigHelper, SparkConfigHelper}
import com.itheima.dmp.etl.IPProcessor
import com.itheima.dmp.utils.SparkSessionUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

/// TODO: 获取json数据进行ETL处理,添加省市、经纬度信息,删除重复字段,保存到kudu中
object PmtETLRunner {

  def main(args: Array[String]): Unit = {

    /// TODO: 1.构建sparkSession实例对象
    val spark: SparkSession = SparkSessionUtils.createSparkSession(PmtETLRunner.getClass)

    /// TODO: 2.读取json格式的数据
    val dataFrame: DataFrame = spark.read.json(AppConfigHelper.AD_DATA_PATH)

    /// TODO: 3.调用ETL处理方法
    val kuduDF: DataFrame = IPProcessor.processData(dataFrame)

    /// TODO: 4.保存到kudu表
    import com.itheima.dmp.utils.KuduUtils._

    //a.创建表 表的名称 -> ods_adinfo_20190914  -> 每天一个表
    val tableName: String = AppConfigHelper.AD_MAIN_TABLE_NAME
    val schema: StructType = kuduDF.schema
    // 主键必须是schema中的字段
    val keys: Seq[String] = Seq("uuid")
    spark.createKuduTable(tableName, schema, keys)

    //b.保存数据
    kuduDF.saveAsKuduTable(tableName)

    //Thread.sleep(100000)

    /// TODO: 5.应用完成,关闭资源
    spark.stop()
  }

}
