package com.itheima.dmp.test

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkGraphTest {

  def main(args: Array[String]): Unit = {

    // 1. 构建sparksession对象
    val (spark, sc) = {
      //a.创建sparkConf对象
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[3]")

      //b.构造者模式创建sparksession实例对象
      val session = SparkSession.builder()
        //可以传入kv配置信息  也可以直接传入sparkConfig
        .config(sparkConf)
        .getOrCreate()

      //c.返回spark实例
      (session, session.sparkContext)
    }

    // 2. 图中顶点数据 -> VertexId就是Long类型的别称  ->  顶点的属性是二元组
    val userVertices: RDD[(VertexId, (String, String))] = sc.parallelize(
      List(
        (3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
        (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))
      )
    )

    // 3. 图中边的数据,边由两个顶点和属性组成
    /**
      * case class Edge[@specialized(Char, Int, Boolean, Byte, Long, Float, Double) ED] (
      *     - var srcId: VertexId = 0,                 ->  源顶点的id
      *     - var dstId: VertexId = 0,                 ->  目标顶点的id
      *     - var attr: ED = null.asInstanceOf[ED])    ->  边的属性
      */
    val relationshipEdges: RDD[Edge[String]] = sc.parallelize(
      List(
        Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")
      )
    )

    // 4. 使用顶点和边构建图
    // 默认的顶点属性值
    val defaultUserAttr: (String, String) = ("John Doe", "Missing")

    //构建图
    /**
      * def apply[VD: ClassTag, ED: ClassTag](
      *     - vertices: RDD[(VertexId, VD)],
      *     - edges: RDD[Edge[ED] ],
      *     - defaultVertexAttr: VD = null.asInstanceOf[VD],
      *     - edgeStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY,
      *     - vertexStorageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
      * ): Graph[VD, ED]
      */
    val graph: Graph[(String, String), String] = Graph(
      userVertices,
      relationshipEdges,
      defaultVertexAttr = defaultUserAttr
    )

    //缓存图
    graph.cache()

    //获取所有顶点的数据
    graph.vertices.foreach { case (id, (name, pos)) => println(s"$id $name -> $pos") }

    //获取所有边数据
    graph.edges.foreach { case (Edge(srcId, dstId, attr)) => s"$srcId -> $dstId 属性 ->  $attr" }

    //获取图中顶点属性为教授（prof）用户数
    val profCount: VertexId =graph.vertices.filter { case (_, (_, pos)) => "prof".equals(pos) }.count()

    println(s"教授的个数为 -> $profCount")

    //两个顶点与边组成一个实体EdgeTriplet
    /**
      * 源端 -> (rxin,student)    终端 -> (jgonzal,postdoc)    边的属性 -> collab
      * 源端 -> (istoica,prof)    终端 -> (franklin,prof)      边的属性 -> colleague
      * 源端 -> (franklin,prof)   终端 -> (rxin,student)       边的属性 -> advisor
      * 源端 -> (franklin,prof)   终端 -> (jgonzal,postdoc)    边的属性 -> pi
      */
    graph.triplets.foreach{triplets=>
      println(s"源端 -> ${triplets.srcAttr} 终端 -> ${triplets.dstAttr} 边的属性 -> ${triplets.attr}")
    }

    //释放缓存
    graph.unpersist()

    //Thread.sleep(100000)

    // 关闭资源
    sc.stop()

  }

}
