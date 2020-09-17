package com.wdcloud.graphx.modelTraining.graph.util

import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.VertexId
import com.wdcloud.graphx.modelBuild.graph.ReadDataFromHbase

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap
import it.unimi.dsi.fastutil.longs.Long2ObjectMap
import it.unimi.dsi.fastutil.objects.ObjectArrayList
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.immutable.ParSeq
import scala.collection.parallel.mutable.ParArray
import com.wdcloud.graphx.pojo.UserMapInfo

object GraphResultHandle extends Serializable {

  /**
   * 对聚合到顶点上的最终的推荐结果进行处理，过滤掉邻居顶点，并对边进行合并，分数进行累加
   * @param:	oldAttr: Map[Int, Object]:每个顶点上的属性值，是map结构，map中使用kv格式保存着多个属性对
   * @param:  newAttr: List[(VertexId, (Int, Double))]:sendMsg发送来的数据。--->List[(顶点id, (顶点类型, score))]
   * @param:  vId: VertexId: 当前顶点hash后的id
   * @return: Map[Long, List[(VertexId, Double)]]-->Map[推荐物品类型, List[(被推荐物品内部id, score)]]
   */
  def finallyPointResultHandle(
      oldAttr: Map[Int, Any], 
      newAttr: Array[(VertexId, (Long, Double))], 
      vId: VertexId): Map[Long, Array[(VertexId, Double)]] = {
    
    val neiID: Int = UserMapInfo.pointAttrMap.get("neiborId").get
    
    /**
     * 对finallyPointResultHandle函数中的集合进行聚合
     */
    val aggregationRecListScore = (list: Array[(VertexId, (Long, Double))]) => {
      var score: Double = 0
      var recType: Long = 0
      for (info: (VertexId, (Long, Double)) <- list) {
        score = score + info._2._2
        recType = info._2._1
      }
      (recType, score)
    }

    //去除掉，推荐结果中的顶点的邻居顶点
    val neiborIds = oldAttr.apply(neiID).asInstanceOf[Array[VertexId]]
    val dealData = newAttr
      .filter(x => !neiborIds.contains(x._1) && vId != x._1)
      .groupBy(_._1)
      .mapValues(aggregationRecListScore)  
      .groupBy(x => x._2._1)
      .mapValues(x => x.map(x => (x._1, x._2._2)).toArray)

    val result = Map[Long, Array[(VertexId, Double)]]()
    result.++=(dealData)
  }

  /**
   * 对pregel迭代出来的结果进行处理
   * 		1. 对指定类型的顶点进行推荐(比如这里给person类型顶点进行推荐)
   * 		2. 对那些key是"rec",value值不是Map的赋值为Map[String, List[(String, Double)]]()
   * @param Graph[Map[Int, Object], Double]-->图模型
   * @return RDD[(VertexId, (String, Map[String, List[(String, Double)]]))]-->RDD[(用户innerId, (用户业务id, Map[被推荐物品的类型, List[(被推荐给物品的业务id, score)]]))]
   */
  def recResultHandle(graph: Graph[Map[Int, Any], Double]): RDD[(VertexId, Map[Long, Array[(Long, Double)]])] = {
    val resultDatas = graph.vertices.filter(x => x._2.apply(1).asInstanceOf[Long] == 7986692084083535784L) //测试数据集中01(person)映射
    val recID: Int = UserMapInfo.pointAttrMap.get("rec").get
    val resultData = resultDatas.map(x =>
      try {
        if (x._2.apply(recID).isInstanceOf[Map[Long, Array[(Long, Double)]]]) { 
          (x._1, x._2.apply(recID).asInstanceOf[Map[Long, Array[(Long, Double)]]])
        } else {
          (x._1, Map[Long, Array[(Long, Double)]]())
        }
      } catch {
        case ex: NoSuchElementException => (x._1, Map[Long, Array[(Long, Double)]]())
      }).cache()
    graph.unpersistVertices(blocking = false)  
    graph.edges.unpersist(blocking = false)
    resultData
  }

  /**
   * 参数代表同一个用户下的基于圈子的推荐信息和基于好友的推荐信息
   * 	@param data1 Map[Long, List[(Long, Double)]]-->Map[type, List[(被推荐物品业务id, score)]]
   *  @return Map[Long, List[(Long, Double)]]
   *
   * 将两个map进行合并，条件是相同的用户id，对list进行合并，并对合并后的list进行分数累加和去重
   *
   * 之所以开始会将Map转化为list，原因是，将两个map合并到一起的时候，相同的key会进行覆盖，但是我们不希望后边的将前边的覆盖掉，所以转化为list进行计算
   */
  def mapCombine(
      data1: Map[Long, Array[(Long, Double)]], 
      data2: Map[Long, Array[(Long, Double)]]): Map[Long, Array[(Long, Double)]] = {
    //1.将两个推荐结果map先转化为list，然后合并到一起
    val list1 = data1.toList.:::(data2.toList).par

    /**
     * 对保存了相同id的list集合中的score进行累加 
     * param list 保存了顶点id和顶点对应score的集合
     * return 总的score
     */
    val aggregationScore = (list: Array[(Long, Double)]) => {
      var score: Double = 0
      for (info: (Long, Double) <- list) {
        score = score + info._2
      }
      score
    }  

    /**
     * 对同一种类型的推荐的结果集list进行聚合
     */
    val aggregationRecList = (list: ParSeq[(Long, Array[(Long, Double)])]) => {
      var mumList = new ListBuffer[(Long, Double)]()
      for (info: (Long, Array[(Long, Double)]) <- list) { 
        mumList.++=(info._2)
      }
      mumList.toArray
    }

    //2.对list集合按照类型进行分组,同一个用户的相同类型的推荐结果分到了一组，并组成一个list,所以这里就需要对同一类型的推荐结果组成的list进行处理   
    val result = list1
      .groupBy(x => x._1)
      .mapValues(aggregationRecList) 
      .map { x =>
        val addScoreData = x._2.groupBy(_._1).mapValues(aggregationScore).toArray.sortBy(x => x._2).takeRight(10)
        (x._1, addScoreData)
      }
    val mmuMap: Map[Long, Array[(Long, Double)]] = Map[Long, Array[(Long, Double)]]()
    mmuMap.++=(result.toList) 
  }

  /**
   * 将推荐结果中的顶点类型和顶点id转化为对应的String类型的业务类型
   */
  def finallyResultHandle(result: RDD[(Int, Long, Map[Long, Array[(Long, Double)]])]): RDD[(Int, String, Map[String, Array[(String, Double)]])] = {
    val pointIdMap = UserMapInfo.pointIdMap
    val pointTypeMap = UserMapInfo.pointTypeMap
    val pointAttrMap = UserMapInfo.pointAttrMap  
    val handleResult = result.map { x =>
      val recMap = x._3.map { mapInfo =>
        val recType = pointTypeMap.apply(mapInfo._1)
        val recList = mapInfo._2.map(x => (pointIdMap.apply(x._1), x._2))
        (recType, recList)
      }

      (x._1, pointIdMap.apply(x._2), recMap)
    }

    handleResult
  }
}