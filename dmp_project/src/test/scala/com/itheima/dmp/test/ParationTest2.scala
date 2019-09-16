package com.itheima.dmp.test

import org.apache.spark.sql.SparkSession

object ParationTest2 {

  def main(args: Array[String]): Unit = {


      val spark = SparkSession.builder()
        .appName("TestPartitionNums")
        .master("local")
        .config("spark.default.parallelism", 8)
        .getOrCreate()

      val sc = spark.sparkContext

      println("默认的并行度: " + sc.defaultParallelism)

    //sc读取数据,封装成RDD  ->   与文件块数 和 文件格式  以及指定的分区数有关
      println("sc.parallelize 默认分区：" + sc.parallelize(1 to 30).getNumPartitions)
      println("sc.parallelize 参数指定，参数大于sc.defaultParallelism时：" + sc.parallelize(1 to 30, 100).getNumPartitions) //100
      println("sc.parallelize  参数指定，参数小于sc.defaultParallelism时：" + sc.parallelize(1 to 30, 3).getNumPartitions) //3

      var data = Seq((1, 2), (1, 2), (1, 2), (1, 2), (1, 2))

    //spark读取数据,封装成DF ->与默认的并行度 和 文件块数 文件个数有关
      println("spark.createDataFrame data的长度小于sc.defaultParallelism时，长度：" + data.length + " 分区数：" + spark.createDataFrame(data).rdd.getNumPartitions) //5
      data = Seq((1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2), (1, 2))
      println("spark.createDataFrame data的长度大于sc.defaultParallelism时，长度：" + data.length + " 分区数：" + spark.createDataFrame(data).rdd.getNumPartitions) //8

      spark.stop

  }
}
