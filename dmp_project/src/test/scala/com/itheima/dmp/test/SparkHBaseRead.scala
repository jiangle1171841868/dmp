package com.itheima.dmp.test

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 从HBase读数据
  */
object SparkHBaseRead {

  def main(args: Array[String]): Unit = {


    // a. 构建SparkContext实例对象
    val sparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripPrefix("$"))
      .setMaster("local[4]")
      // 设置使用Kryo序列
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 注册哪些类型使用Kryo序列化, 最好注册RDD中类型
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
    val sc: SparkContext = new SparkContext(sparkConf)


    // b. 从HBase表中读取数据  -> 一个region对应一个分区

    /**
      * def newAPIHadoopRDD[K, V, F <: NewInputFormat[K, V]](
      *     -  conf: Configuration = hadoopConfiguration,   //  配置文件
      *     -  fClass: Class[F],                            //  读取数据的存储格式 ->  就是读取文件的类
      *     -  kClass: Class[K],                            //  key的类型
      *     -  vClass: Class[V]                             //  value的数据类型
      * ): RDD[(K, V)]
      */

    val conf: Configuration = HBaseConfiguration.create()
    // 设置读HBase表的名称 配置属性在里面找   mapreduce.TableInputFormat  新的MR  API
    conf.set(TableInputFormat.INPUT_TABLE, "userState")


    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )

    // hbaseRDD.foreach(println)

    // 遍历获取rowKey和列族 列名 列值
    hbaseRDD.take(5).foreach { case (rowKey, result) =>

      println(s"rowKey -> ${Bytes.toString(rowKey.get())}")

      // 通过结果集result获取所有的cell
      val cells: Array[Cell] = result.rawCells()

      for (cell <- cells) {

        // 使用CellUtil工具类获取数据
        // a. 列族
        val family: String = Bytes.toString(CellUtil.cloneFamily(cell))
        // b. 列名
        val qualifier: String = Bytes.toString(CellUtil.cloneQualifier(cell))
        // c. 列值
        val value: String = Bytes.toString(CellUtil.cloneValue(cell))
        // d. 获取版本号
        cell.getTimestamp
        println(s"\t ${family}:${qualifier} = ${value}, version -> ${cell.getTimestamp}")
      }


    }

    sc.stop()
  }

}
