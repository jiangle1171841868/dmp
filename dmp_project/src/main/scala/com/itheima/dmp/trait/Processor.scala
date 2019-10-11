package com.itheima.dmp.`trait`

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame

/// TODO: 数据处理接口   DataDrame  ->  DataFrame
trait Processor extends Logging{

  /// TODO: 对DataFrame数据集的处理转化
  def processData(dataframe: DataFrame): DataFrame

}
