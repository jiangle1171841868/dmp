package com.itheima.dmp.tags

import com.itheima.dmp.`trait`.Processor
import com.itheima.dmp.beans.{IdsWithTags, UserTags}
import com.itheima.dmp.utils.TagUtils
import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * 标签的合并（今日ODS表中将数据标签与昨日用户标签进行合并，涉及统一用户识别）
  */
object MergeTagsProcessor extends Processor {

  override def processData(allTagsDF: DataFrame): DataFrame = {

    // 1. 获取SparkSession实例对象，导入隐式转换和函数库
    val spark = allTagsDF.sparkSession
    import spark.implicits._

    // 2. 将DataFrame转化为DataSet -> DataFrame + 外部数据表类型 转化为DataSet  -> 再转化为RDD -> RDD内部数据结构就不是Rowm,是样例类便于操作数据
    val rallTagsRDDdd: RDD[IdsWithTags] = allTagsDF.as[IdsWithTags].rdd

    /// TODO: 针对allTagsRDD数据集构建图，获取连通图，进行统一用户识别和数据标签的合并

    //
    /**
      * 3. 创建图中顶点RDD
      *    - key
      *         - 主标识ID : mainid   ->   key就是主标识ID的HashCode值
      *         - ids标识  : ids      ->   key是名称和值拼接的字符串的HashCode值  格式  ->  s"$idName->$idValue"
      *    - value
      *         - 主标识ID : mainid   ->   value                               格式  ->   mainid##$mainid##$tagsStr   -> 主标识符携带标签数据
      *         - ids标识  : ids      ->   value  ->   名称和值拼接的字符串      格式  ->   s"$idName->$idValue"
      *
      *    - 代码实现
      *         - 对每一条数据进行处理  ->   返回多条数据(多个顶点)  ->  使用flatMap
      *         - 一条数据转化成多条数据     使用list集合收集
      */
    val verticesRDD: RDD[(VertexId, String)] = rallTagsRDDdd.flatMap { case IdsWithTags(mainId: String, idsMap: Map[String, String], tagsMap: Map[String, Double]) =>

      // a. 将tagsMap转化为字符串
      val tagsStr: String = TagUtils.map2Str(tagsMap)

      // b. 创建集合,收集顶点
      val list = new ListBuffer[(VertexId, String)]()

      // c. 构建主标识mainid的顶点
      list += mainId.hashCode.toLong -> s"mainid##$mainId##$tagsStr"

      // d. 构建ids标识的顶点
      idsMap.foreach { case (idName, idValue) =>

        val idVertex = s"$idName->$idValue"

        //添加到list集合
        list += idVertex.hashCode.toLong -> idVertex
      }
      // 将list转化Wie不可变集合返回
      list.toList
    }.distinct()
    verticesRDD.persist(StorageLevel.MEMORY_AND_DISK) // 缓存RDD
    println(s"构建数据标签的顶点数目: ${verticesRDD.count()}") // 触发缓存


    /**
      * 4. 创建边RDD
      *
      *    -  边的数据封装到Edge中   ->  srcId, dstId, edgeAttr
      *    -  Edge[String] 里面的泛型就是边属性的类型   可以是这些类型 ->  (Char, Int, Boolean, Byte, Long, Float, Double)
      *       - srcId  源Id        ->  主标识Id  mainId
      *       - dstId 目标Id       ->   ids标识
      *       - edgeAttr 边得属性  ->  自定义 可有可无
      */

    val edgesRDD: RDD[Edge[String]] = rallTagsRDDdd.flatMap { case IdsWithTags(mainId: String, idsMap: Map[String, String], tagsMap: Map[String, Double]) =>

      // a. 构建集合,集合的泛型是Edge
      val list = new ListBuffer[Edge[String]]()

      // b. srcId
      val srcId = mainId.hashCode.toLong

      // c. dstId
      idsMap.foreach { case (idName, idValue) =>

        val idVertex = s"$idName->$idValue"
        val dstId = idVertex.hashCode.toLong

        //封装成Edge,添加到list集合中
        list += Edge(srcId, dstId, "dmp")

      }
      list.toList
    }
    edgesRDD.persist(StorageLevel.MEMORY_AND_DISK) // 缓存RDD
    println(s"构建数据标签的边数目: ${edgesRDD.count()}") // 触发缓存

    // 5. 构建图
    val graph = Graph(verticesRDD, edgesRDD)

    // 6. 构建连通图
    val ccGraph: Graph[VertexId, String] = graph.connectedComponents()

    /**
      * 获取的连通图中只有顶点Id,需要join原图数据集获取顶点的属性
      */

    // a.获取原图中顶点数据集
    val verticesGraphRDD: VertexRDD[String] = graph.vertices

    // b. 将连通图中的顶点数据集与原图中顶点数据集join  -> join 使用与k v类型的RDD
    val userTagsRDD: RDD[UserTags] = ccGraph.vertices
      .join(verticesGraphRDD) //RDD[(VertexId, (VertexId, String))]
      .map { case ((vertexId, (ccVertexId, vertexAttr))) => (ccVertexId, (vertexId, vertexAttr)) }
      .groupByKey()
      .values //RDD[Iterable[(VertexId, String)]]
      .map { iter =>

      // 多次使用迭代器中的数据，将迭代器转换为列表   -> 顶点数据
      val list: List[(VertexId, String)] = iter.toList

      // a. 合并标签：将多个数据标签合并，最终形成用户标签
      val tagsStr: String = list
        // 过滤出主标识符的顶点 -> 主标识符的顶点中有标签数据tags
        .filter { case (vertexId, vertexAttr) => vertexAttr.startsWith("mainid##") }
        //vertexAttr数据类型  ->  "mainid##$mainId##$tagsStr"
        .map { case (vertexId, vertexAttr) => vertexAttr.split("##")(2) }
        //标签聚合
        .reduce((temTags, tags) => {

        // 将tags转化为Map集合
        val tmpTagsMap: Map[String, Double] = TagUtils.tagsStr2Map(temTags)
        val tagsMap: Map[String, Double] = TagUtils.tagsStr2Map(tags)

        /**
          * 标签合并
          *   - 1. 以tmpTagsMap为基准
          *   - 2. 遍历tmpTagsMap的key,tagsMap中含有key就将权重相加合并,没有key就直接返回
          *   - 3. 最后tagsMap中有,tmpTagsMap中没有的key,需要添加进去,使用Map集合 ++ 后面的会覆盖前面已经有的,没有就添加 -> 将tagsMap的剩余数据合并在一起
          */

        // 获取tmpTagsMap的key
        val tmpMap: Map[String, Double] = tmpTagsMap.map { case (tagName, tagWeight) =>

          // 判断tagsMap中有没有这个key
          if (tagsMap.contains(tagName)) {

            //有相同的标签就合并标签 -> 权重相加
            tagName -> (tmpTagsMap(tagName) + tagWeight)

          } else {

            //没有相同的标签,直接返回tmpTagsMap的标签
            tagName -> tagWeight
          }
        }

        // 标签合并返回,返回字符串,接下来需要封装到UserTags中,保存到kudu中
        TagUtils.map2Str(tagsMap ++ tmpMap)
      })

      // b. 合并所有的标识符IDs（非主标识符ID）
      val idsMap: Map[String, String] = list
        // 过滤出主标识符的顶点 -> 主标识符的顶点中有标签数据tags
        .filter { case (vertexId, vertexAttr) => !vertexAttr.startsWith("mainid##") }
        // 获取表示符id和value,数据类型 ->  s"$idName->$idValue"
        .map { case ((vertexId, vertexAttr)) =>

        val Array(idName, idValue) = vertexAttr.split("->")

        // 返回
        idName -> idValue
      }.toMap

      // c. 找一个代表用户标识符ID：从主标识符mainId中获取一个即可
      val main_id: String = list
        .filter { case (vertexId, vertexAttr) => vertexAttr.startsWith("mainid##") }
        //随便获取一个就可以作为mainId
        .head._2.split("##")(2)

      //返回样例类
      UserTags(main_id, TagUtils.map2Str(idsMap), tagsStr)

    }

    // 返回DF
    userTagsRDD.toDF()
  }

}
