package com.wdcloud.graphx.job

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.GraphOps
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import scala.collection.mutable.LinkedList
import com.sun.corba.se.impl.io.TypeMismatchException
import com.wdcloud.graphx.javaUtil.Configuration
import com.wdcloud.graphx.modelBuild.graph.GraphModel
import com.wdcloud.graphx.modelBuild.graph.ReadDataFromHbase
import com.wdcloud.graphx.modelBuild.graph.create.CreateGraphModelFromEdgeByHbase
import com.wdcloud.graphx.modelTraining.Recommender
import com.wdcloud.graphx.modelTraining.graph.util.GraphResultHandle
import com.wdcloud.graphx.resultHandle.graph.SaveResult
import com.wdcloud.graphx.resultHandle.graph.SaveResultToHbase
import com.wdcloud.graphx.scalaUtil.HbaseUtil
import com.wdcloud.graphx.scalaUtil.ScalaRefUtil
import collection.JavaConverters._

import akka.event.slf4j.Logger
import com.wdcloud.graphx.modelBuild.graph.ReadDataFromHbase
import com.wdcloud.graphx.modelTraining.graph.TwoDegreesFriendsRecommender
import com.wdcloud.graphx.modelTraining.graph.MixCommunityRecommender
import com.wdcloud.graphx.resultHandle.graph.SaveResultToText
import org.apache.spark.graphx.Edge
import com.wdcloud.graphx.modelBuild.graph.create.CreateGraphModelByTxt
import com.wdcloud.graphx.dataPretreatment.DataPrep

/**
 * 任务的执行调度
 */
class Job(val sc: SparkContext) extends Serializable {
  var graph: Graph[Map[Int, Any], Double] = null
  var finallyResult: RDD[(Int, String, Map[String, Array[(String, Double)]])] = null
  var userConf: Map[String, String] = null
  val logger = Logger(this.getClass.getName)

  /**
   * 运行任务
   */
  def run(conf: Configuration) {
    logger.warn("任务开始调用")
    DataPrep.dataPrep(sc, conf)
    dataPrep(conf)
    greateModel(conf)
    trainModel(conf)
    handleResult(conf)
    logger.warn("任务执行完成")

  }

  /**
   * 对用户配置信息进行预处理
   */
  def dataPrep(conf: Configuration) {

    //根据用户的三项配置标识信息，从基础表中读出对应的实现类的路径名，并返回
    val baseInfo = ReadDataFromHbase.readBasedInfo(sc, conf).first

    //将三项配置的类的全路径名和步骤名称组成kv对重新写入到conf中作为最终的算法流程中使用的配置对象
    conf.set("greateModelClassName", baseInfo._1)
    conf.set("trainModelClassName", baseInfo._2)
    conf.set("saveResultClassName", baseInfo._3)
    
    //将conf中的信息写入到hbase中的用户配置表USER_CONF_TABLE
    HbaseUtil.writeUserConfInfo(conf, "JANUARY:T_USER_CONF")
    userConf = conf.toMap(conf).asScala
    logger.warn("数据预处理完成")
  }

  /**
   * 生成图模型
   */
  def greateModel(conf: Configuration) {
    //获取数据源类型，通过数据源类型得到试用那种方式生成图模型
    val className = conf.get("greateModelClassName")
    var instObject: GraphModel = null
    //根据数据源类型，使用反射创建生成图的实例对象 
    try {
      val inst = ScalaRefUtil.newInstance(className).asInstanceOf[GraphModel]
      if (inst.isInstanceOf[GraphModel]) {
        instObject = inst.asInstanceOf[GraphModel]
      } else {
        throw new TypeMismatchException
      }
      //调用对象方法得出结果
      graph = instObject.createGraph(sc, conf)
    } catch {
      case e: Throwable => e.printStackTrace() // TODO: handle error
    }
  }

  /**
   * 训练模型（使用配置好的算法对模型进行训练）
   */
  def trainModel(conf: Configuration) {
    //获取算法标识
    val className = conf.get("trainModelClassName")
    var instObject: Recommender = null

    try {
      val inst = ScalaRefUtil.newInstance(className, userConf)
      if (inst.isInstanceOf[Recommender]) {
        instObject = inst.asInstanceOf[Recommender]
        logger.warn("推荐算法对象是：" + instObject.toString())
      } else {
        throw new TypeMismatchException
      }
      //调用对象方法得出结果
      val middleResult = instObject.predict(sc, graph)
      val result = middleResult.reduceByKey((x, y) => GraphResultHandle.mapCombine(x, y)).map(x => (0, x._1, x._2))
      finallyResult = GraphResultHandle.finallyResultHandle(result)
      logger.warn("推荐结果处理完成")
    } catch {
      case e: Throwable => e.printStackTrace() // TODO: handle error
    }

    logger.warn("推荐结果处理完成")

  }

  /**
   * 对模型训练的结果进行处理
   */
  def handleResult(conf: Configuration) {
    //获取推荐结果保存算法标示符
    val className: String = conf.get("saveResultClassName")
    //将推荐结果进行持久化
    var instObject: SaveResult = null
    try {
      val inst = ScalaRefUtil.newInstance(className, userConf)
      if (inst.isInstanceOf[SaveResult]) {
        instObject = inst.asInstanceOf[SaveResult]
      } else {
        throw new TypeMismatchException
      }
      //调用对象方法得出结果
      instObject.saveResult(finallyResult)
    } catch {
      case e: Throwable => e.printStackTrace() // TODO: handle error 
    }
  }
}