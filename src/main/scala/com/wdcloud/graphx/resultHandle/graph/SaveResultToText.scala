package com.wdcloud.graphx.resultHandle.graph

import com.wdcloud.graphx.scalaUtil.FileOperate
import org.apache.spark.rdd.RDD
import akka.event.slf4j.Logger
import com.wdcloud.graphx.javaUtil.Configuration
import com.wdcloud.graphx.environmentContext.DataContext
import scala.collection.mutable.Map
/**
 * 将结果保存到text文件中
 */
class SaveResultToText(userConf: Map[String, String]) extends DataContext(userConf)  with SaveResult{     
  val logger = Logger(this.getClass.getName)   
  /**
   * 将推荐结果持久化到text文件
   * param:
   * 		RDD[(String, String, Map[String, List[(String, Double)]])]-->RDD[(命名空间，用户业务id, Map[推荐物品类型, List[(推荐物品业务id, Double)]])]
   */
  override def saveResult(result: RDD[(Int, String, Map[String, Array[(String, Double)]])]){
    val path = userConf.get("rec.result.path").get
    FileOperate.saveResultToText(path, result)  
    logger.warn("结果成功保存到result.txt")     
  }  
}  