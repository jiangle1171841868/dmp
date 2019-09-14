package com.itheima.dmp.`trait`

import org.apache.spark.sql.DataFrame

/// TODO: 数据ETL处理接口
trait Processor {

  /// TODO: 对DataFrame数据集的处理转化
  def processData(dataframe: DataFrame): DataFrame

}
