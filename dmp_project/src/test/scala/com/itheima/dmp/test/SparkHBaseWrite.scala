package com.itheima.dmp.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 向HBase写数据
  */
object SparkHBaseWrite {

  def main(args: Array[String]): Unit = {

    // a. 构建SparkContext实例对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripPrefix("$"))
      .setMaster("local[4]")
      // 设置序列化
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册RDD中的类型
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Put]))

    val sc: SparkContext = new SparkContext(sparkConf)

    // b. 模拟数据集
    val inputRDD: RDD[(String, Int)] = sc.parallelize(
      List(("spark", 233), ("hadoop", 123), "flink" -> 300),
      numSlices = 2
    )

    /**
      * 构建 HBase表5要素
      *   - 表名   spark_hbase
      *   - rk     word
      *   - 列族   info
      *   - 列名   "count"
      *   - 列值   count
      */

    // 将存储到HBase中的RDD进行转化 ->RDD[(ImmutableBytesWritable, Put)]
    val hbaseRDD: RDD[(ImmutableBytesWritable, Put)] = inputRDD.map { case (word, count) =>

      // 构建HBase表的rk
      val rowKey = new ImmutableBytesWritable(Bytes.toBytes(word))

      // 创建Put对象,封装数据
      val put = new Put(rowKey.get())
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count + ""))

      // 返回二元组
      rowKey -> put
    }


    // 保存数据到HBase表
    /**
      * def saveAsNewAPIHadoopFile(
      *     - path: String,                                           // MR的输出路径,必须不存在 -> 这里是保存到HBase表  在这里没有用
      *     - keyClass: Class[_],
      *     - valueClass: Class[_],
      *     - outputFormatClass: Class[_ <: NewOutputFormat[_, _]],
      *     - conf: Configuration = self.context.hadoopConfiguration
      * ): Unit
      *
      *     - 方法底层就是MR程序
      * val hadoopConf = conf
      * val job = NewAPIHadoopJob.getInstance(hadoopConf)
      *         job.setOutputKeyClass(keyClass)
      *         job.setOutputValueClass(valueClass)
      *         job.setOutputFormatClass(outputFormatClass)
      * val jobConfiguration = job.getConfiguration
      *         jobConfiguration.set("mapreduce.output.fileoutputformat.outputdir", path)
      * saveAsNewAPIHadoopDataset(jobConfiguration)
      */

    val conf: Configuration = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, "spark-hbase-wordcount")
    hbaseRDD.saveAsNewAPIHadoopFile(
      "spark-hbase" + System.currentTimeMillis(),
      classOf[ImmutableBytesWritable],
      classOf[Put],
      //public class TableOutputFormat<KEY> extends OutputFormat<KEY, Mutation>  -> 需要key的泛型
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

    // 应用结束，关闭资源
    sc.stop()

  }
}
