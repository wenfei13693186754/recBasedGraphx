package com.wdcloud.graphx.resultHandle.graph

import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import com.wdcloud.graphx.environmentContext.DataContext
import com.wdcloud.graphx.scalaUtil.DataToJson
import com.wdcloud.graphx.scalaUtil.HbaseUtil

import akka.event.slf4j.Logger

/**
 * 将推荐结果保存到hbase中
 */
class SaveResultToHbase(userConf: Map[String, String]) extends DataContext(userConf)  with SaveResult {  
  
  val logger = Logger(this.getClass.getName)    
  override def saveResult(result: RDD[(Int, String, Map[String, Array[(String, Double)]])]){

    val namespace = userConf.get("namespace").get
    val resultTable = userConf.get("rec.result.table").get
    
    val jsonResult = DataToJson.recResultRDDListToJson(result)   
    
    HbaseUtil.writeAllRecInfo(namespace, resultTable, jsonResult)
    logger.warn("将基于圈子和基于好友的综合的推荐结果持久化到hbase中成功")  
  }
}  