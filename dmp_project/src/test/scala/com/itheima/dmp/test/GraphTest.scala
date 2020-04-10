package com.itheima.dmp.test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

object GraphTest {

  def main(args: Array[String]): Unit = {


    // 1. 构架spark实例对象
    val sc = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

      //b.构建sparkSession实例对象返回
      SparkContext.getOrCreate(sparkConf)
    }

    // 2. 模拟数据   ->  mainid  ids
    val usersRDD: RDD[(String, Map[String, String])] = sc.parallelize(
      List(
        ("M001", Map(("imei", "IM001"), ("mac", "MA001"), ("openudid", "OU001"), ("idfa", "ID001"))),
        ("M002", Map(("mac", "MA002"), ("openudid", "OU001"))),
        ("M003", Map(("imei", "IM003"), ("androidid", "AN003"), ("idfa", "ID001"))),
        ("M004", Map(("imei", "IM004"), ("mac", "MA004"), ("openudid", "OU004"))),
        ("M005", Map(("androidid", "AN005"), ("idfa", "ID005")))
      )
    )

    /**
      * 3. 创建图中顶点RDD
      *    - 顶点 -> (顶点ID,顶点属性)  ->  key表示顶点ID，value表示顶点的属性
      *    - key
      *         - 主标识ID : mainid   ->   key就是主标识ID的HashCode值
      *         - ids标识  : ids      ->   key是名称和值拼接的字符串的HashCode值  格式  ->  s"$idName->$idValue"
      *    - value
      *         - 主标识ID : mainid   ->   value                               格式  ->   mainid##$mainid
      *         - ids标识  : ids      ->   value  ->   名称和值拼接的字符串      格式  ->   s"$idName->$idValue"
      *
      *    - 代码实现
      *         - 对每一条数据进行处理  ->   返回多条数据(多个顶点)  ->  使用flatMap
      *         - 一条数据转化成多条数据     使用list集合收集
      */
    val verticesRDD: RDD[(VertexId, String)] = usersRDD.flatMap { case (mainId, idsMap) =>

      // a. 创建一个集合收集转化后的每一个顶点
      val list = new ListBuffer[(VertexId, String)]()

      // b. 构建主标识符mainId的顶点数据
      list += mainId.hashCode.toLong -> s"main##$mainId"

      // c. 构建ids标识符的顶点数据  ->   对map集合进行处理,获取顶点,不需要返回值  ->  可以使用foreach
      idsMap.foreach { case (idName, idValue) =>

        val idVertex = s"$idName->$idValue"

        list += idVertex.hashCode.toLong -> idVertex
      }

      // d. 返回顶点list集合  -> 转化为不可变list集合
      list.toList
    }.distinct() // 去重
    verticesRDD.persist(StorageLevel.MEMORY_AND_DISK) // 缓存RDD
    verticesRDD.count() // 触发缓存

    /**
      * 4. 创建边RDD
      *
      *    -  边的数据封装到Edge中   ->  srcId, dstId, edgeAttr
      *    -  Edge[String] 里面的泛型就是边属性的类型   可以是这些类型 ->  (Char, Int, Boolean, Byte, Long, Float, Double)
      *       - srcId  源Id        ->  主标识Id  mainId
      *       - dstId 目标Id       ->   ids标识
      *       - edgeAttr 边得属性  ->  自定义 可有可无
      */
    val edgesRDD: RDD[Edge[String]] = usersRDD.flatMap { case (mainId, idsMap) =>

      // a. 创建一个集合收集转化后的边
      val list = new ListBuffer[Edge[String]]()

      // b. srcId
      val srcId = mainId.hashCode.toLong

      // c. dstId
      idsMap.foreach { case (idName, idValue) =>

        // 获取dstId
        val dstId = s"$idName->$idValue".hashCode.toLong

        // 封装数据到Edge中,添加到List集合
        list += Edge(srcId, dstId, "dmp")
      }
      // d. 转化为不可变集合返回
      list.toList
    }

    edgesRDD.persist(StorageLevel.MEMORY_AND_DISK) // 缓存RDD
    edgesRDD.count() // 触发缓存

    /**
      * 5. 构建图
      * def apply[VD: ClassTag, ED: ClassTag](
      *     - vertices: RDD[(VertexId, VD)],                                  ->    顶点的数据集
      *     - edges: RDD[Edge[ED] ],
      *     - defaultVertexAttr: VD = null.asInstanceOf[VD],                  ->    边的数据集
      *     - edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,      ->    顶点不存在时,顶点属性的默认值
      *     - vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
      * ): Graph[VD, ED]
      */
    val graph: Graph[String, String] = Graph(verticesRDD, edgesRDD)

    // 打印
    println(s"顶点数目 -> ${graph.vertices.count()}")
    println(s"边的数目 -> ${graph.vertices.count()}")


    // 6. 构建连通图  ->  两个字段  ->   (顶点ID,连通组件标识符) ->  连通组件标识符就是连通组件中最小的顶点ID
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents()
    // 获取连通图中组件信息
    ccGraph.vertices.foreach { case (vertexId, ccVertexId) =>
      println(s"顶点id  ->  $vertexId,连通组件id  ->  $ccVertexId")
    }

    // 获取每个连通组件顶点的个数  ->  对连通组件标识符进行分组排序
    val xx = ccGraph.vertices
      .map(_.swap) //将key和value互换
      .groupByKey() //RDD[(VertexId, Iterable[VertexId])]  -> 相同key的value放在一个迭代器里面
      //.values //RDD[Iterable[VertexId]]  ->  获取每个key的所有value
      .foreach { case (vertexId, values) => println(s"$vertexId -> ${values.size}") } // 打印每个连通组件中的顶点个数


    /**
      * 获取的连通图中只有顶点Id,需要join原图数据集获取顶点的属性
      */

    // a. 获取原图中的顶点数据集
    val verticesGraphRDD: VertexRDD[String] = graph.vertices

    // b. 将连通图中的数据集与原图中的顶点数据集关联
    val xxx = ccGraph.vertices // 使用xx方法,随时查看数据类型
      .join(verticesGraphRDD) //RDD[(VertexId, (VertexId, String))]   // key -> 顶点id  value -> (连通标识id,顶点属性)
      // 数据集转化为,(连通标识id,(顶点id,顶点属性))
      .map { case (vertexId, (ccVertexId, vertexAttr)) => (ccVertexId, (vertexId, vertexAttr)) }
      .groupByKey() //RDD[(VertexId, Iterable[(VertexId, String)]}
      .values //RDD[Iterable[(VertexId, String)]]  ->
      .map { iter =>

      iter
        // a. 获取连通组件中主标识mainId对应的数据 ->  过滤数据,找到数据的不同点 -> 主标识数据,属性是main##开头
        .filter { tuple => tuple._2.startsWith("main##") }
        .map { tuple => tuple._2 } //Iterable[String]
        .reduce((tempMainAttr, mianAttr) => s"$tempMainAttr,$mianAttr") // 聚合数据  String
    }.foreach(println)

    // 使用aggregateByKey进行聚合
    ccGraph.vertices
      .join(verticesGraphRDD)
      .map { case (vertexId, (ccVertexId, vertexAttr)) => (ccVertexId, (vertexId, vertexAttr)) }
      // 相同key的value放在迭代器里面,对value进行操作  定义中间临时临时变量的数据类型  ->  也是返回值的数据类型
      .aggregateByKey(new ListBuffer[String])(

      //seqOp: (U, V) => U -> 分区聚合
      (temp: ListBuffer[String], value: (VertexId, String)) => {

        val vertexAttr: String = value._2
        // a. 过滤出主标识mainId对应的数据
        if (vertexAttr.startsWith("main##")) temp += vertexAttr

        // b.返回temp
        temp

      },

      //combOp: (U, U) => U -> 全局聚合
      (u1: ListBuffer[String], u2: ListBuffer[String]) => {
        u1 ++= u2
      }
    ) //RDD[(VertexId, ListBuffer[String])]
      .foreach { case (vertexId, list) => println(list.mkString(", ")) }

    Thread.sleep(100000)

    // 释放缓存
    verticesRDD.unpersist()
    edgesRDD.unpersist()


    // 应用完成,关闭资源
    sc.stop()
  }
}
