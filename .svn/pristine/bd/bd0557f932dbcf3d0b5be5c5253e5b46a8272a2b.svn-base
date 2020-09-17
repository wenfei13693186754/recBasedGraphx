package com.wdcloud.graphx.modelTraining.graph

import scala.Iterator
import scala.collection.mutable.Map
import org.apache.spark.SparkContext
import org.apache.spark.graphx.EdgeDirection
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.VertexId
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

import com.wdcloud.graphx.modelBuild.graph.ReadDataFromHbase
import com.wdcloud.graphx.modelTraining.Recommender
import com.wdcloud.graphx.modelTraining.graph.util.GraphResultHandle
import com.wdcloud.graphx.scalaUtil.PregelUtil

import akka.event.slf4j.Logger
import com.wdcloud.graphx.pojo.UserMapInfo

/**
 * 进行基于二度好友的推荐
 * 	向每个用户推荐他的直接关系好友所关系的物品，但是他没关系的物品
 */
class TwoDegreesFriendsRecommender(userConf: Map[String, String]) extends Recommender(userConf) {
  val logger = Logger(this.getClass.getName)
  var middleResult: RDD[(VertexId, Map[Long, Array[(Long, Double)]])] = null
  
  /**
   * 基于一度好友推荐好友、物、圈子
   * 使用pregel，迭代两次，第一次dst顶点发送消息给src顶点，第二次dst顶点发送它收到的第一次迭代的消息给src顶点
   * 迭代次数使用自定义的PregelUtil中的iterNum来判定
   * 	iterNum初始值是0，每执行一次迭代增加1
   * vprog:
   * 		1.初始化：初始化信息是 可变的List[(VertexId, (Long, Double))]()，vprog函数收到这个信息后，不做任何操作，直接返回oldAttr
   * 		2.第一次迭代收到的消息是List[(VertexId, (Long, Double))]((...))，将其放在key是3(rec)的位置；
   * 		3.第二次迭代收到的消息是List[(VertexId, (Long, Double))]((...))，将其放在key是3(rec)的位置上；
   * 			这个时候要对数组进行处理：包括对相同VertexId的数据进行分数累加去重和分组，最后的处理结果格式是： Map[Long, Array[(VertexId, Double)]]
   *
   * sendMsg：
   * 		1.第一次迭代：发送 Array[(VertexId, (Long, Double))]((...))给src顶点
   * 		2.第二次迭代：限定只有收到第一次迭代消息的顶点才可以发送消息（发送消息的方向设置为IN），发送的消息是第一次迭代收到的消息；
   * 				第二次发送数据时候，要将第一次迭代收到的消息中的分数乘上当前triplet上的score作为最终的score
   *
   * mergeMsg：
   * 		1.第一次迭代，收到的消息是List[(VertexId, (Long, Double))]((...))，将这些消息使用.:::合并为一个List
   * 		2.第二次迭代，收到的消息是List[(VertexId, (Long, Double))]((...))，将这些消息使用.:::合并为一个List
   *
   *  另外迭代过程中使用的集合类型都是可变的集合类型，这样避免了迭代过程中产生多余的中间数据集
   *
   *  @param sc SparkContext
   *  @param graphModel 推荐所使用的图模型
   *  @return RDD[(Int, Long, Map[Long, Array[(Long, Double)]])]-->RDD[(命名空间名称, 用户的inner_Id, Map[推荐物品类型, Array[(推荐物品inner_Id, score)]])]
   */
  val simPregel = new PregelUtil()
  def predict(sc: SparkContext, graphModel: Graph[Map[Int, Any], Double]):  RDD[(VertexId, Map[Long, Array[(Long, Double)]])] = {
    logger.warn("开始基于好友的推荐")
    val graph = sc.broadcast(graphModel)
    //读取配置信息    
    val namespace = userConf.get("namespace").get

    //调用自定义的PregelUtil方法执行pregel迭代操作,这个图已经是缓存好的图
    val pregelGraph = simPregel.apply(graph.value, Array[(VertexId, (Long, Double))](), 2, EdgeDirection.In)(vprog, sendMsg, merge).cache()
    
    middleResult = GraphResultHandle.recResultHandle(pregelGraph)
    logger.warn("基于二度好友的推荐成功")  
    middleResult
  }
 
  /**
   * 基于好友推荐的merge函数
   */
  private def merge(list1: Array[(VertexId, (Long, Double))], list2: Array[(VertexId, (Long, Double))]): Array[(VertexId, (Long, Double))] = {
    val list = list1.++(list2)
    list
  }

  /**
   * 基于好友推荐的vprog函数
   */
  private def vprog(vId: VertexId, oldAttr: Map[Int, Any], newAttr: Array[(VertexId, (Long, Double))]): Map[Int, Any] = {
    val attrID: Int = UserMapInfo.pointAttrMap.get("rec").get
    if (newAttr.size == 0) { //初始化
      oldAttr //Map[被推荐物品的类型, Array[(物品的innerId, Score)]]()
    } else if (simPregel.iterNum == 1) { //第一次迭代,收到的信息格式：Array[(VertexId, (Long, Double))]

      oldAttr.+(attrID -> newAttr)
    } else if (simPregel.iterNum == 2) { //第二次迭代,收到的信息格式：Array[(VertexId, (Long, Double))]-->Array[(被推荐物品hash后的id,, (类型, score))]
      val dealData = GraphResultHandle.finallyPointResultHandle(oldAttr, newAttr, vId)
      oldAttr.+=(attrID -> dealData)
    } else {
 
      oldAttr
    }
  }

  /** 
   * 基于好友推荐的sendMsg函数
   */
  private def sendMsg(triplet: EdgeTriplet[Map[Int, Any], Double]): Iterator[(Long, Array[(VertexId, (Long, Double))])] = {
    
    val recID: Int = UserMapInfo.pointAttrMap.get("rec").get
  
    if (simPregel.iterNum == 0) { //第一次迭代发送的数据格式：Array[(VertexId, (Long, Double))]-->Array[(被推荐物品hash后的id, (类型, Double))]
      val dstAttr = triplet.dstAttr
      Iterator((triplet.srcId, Array[(VertexId, (Long, Double))]((triplet.dstId, (dstAttr.apply(1).asInstanceOf[Long], triplet.attr)))))
    } else { //第二次迭代,限定只有收到第一次迭代消息的顶点才可以发送消息（发送消息的方向设置为IN），发送的消息是第一次迭代收到的消息；
      val dstAttr = triplet.dstAttr
      //第二次发送数据时候，要将第一次迭代收到的消息中的分数乘上当前triplet上的score作为最终的score
      if (dstAttr.get(recID) != None && dstAttr.apply(recID).isInstanceOf[Array[(VertexId, (Long, Double))]]) {
        val itera1 = dstAttr.apply(3).asInstanceOf[Array[(VertexId, (Long, Double))]] //取出每个dst顶点上的map中的3(“rec”)键对应的value，类型是：Array[(VertexId, (Int, Double))],分别代表id和类别以及score类型的数组
        val itera2 = itera1.map(x => (x._1, (x._2._1, x._2._2 * triplet.attr)))

        //将第一次迭代顶点收到的消息置为空，以腾出内存空间  
        dstAttr.+=(3 -> Map[Int, Array[(Long, Double)]]())

        Iterator((triplet.srcId, itera2))
      } else {
        Iterator.empty
      }
    }
  }
}