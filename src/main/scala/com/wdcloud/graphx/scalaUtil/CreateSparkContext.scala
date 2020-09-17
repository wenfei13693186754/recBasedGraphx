package com.wdcloud.graphx.scalaUtil

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CreateSparkContext extends Serializable{
  //private val conf = new SparkConf().setAppName("graphDemo").setMaster("spark://192.168.6.83:7077")
  private val conf = new SparkConf().setAppName("graphDemo").setMaster("local[*]")
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  conf.registerKryoClasses(Array(classOf[com.wdcloud.graphx.environmentContext.DataContext], classOf[com.wdcloud.graphx.javaUtil.Configuration])) 
  @transient private val sc: SparkContext = new SparkContext(conf)  
  def init (): SparkContext = { 
    sc
  }
  def stop (){  
    sc.stop()
  }
}     