package com.wdcloud.graphx.pojo

/**
 * 用户信息在hbase中的配置，包括源数据所在命名空间namespace、行为表edgeTable以及其他配置信息
 */
class UserConf (
  var namespace: String = null,//命名空间
  var confTable: String = null, //用户配置信息表名
  var behTable: String = null, //用户行为表名
  var pAttrTable: String = null,//用户信息表表名
  var iAttrTable: String = null,//物品信息表表名
  
  var userRegId: String = null,//用户注册使用的id
  var serviceId: String = null, //新建业务id
  var sceneId: String = null, //新建场景id
  
  //新建算法模板
  var algoId: String = null,//算法id 
  var algoUserId: String = null, //基于二度好友的算法id
  var algoCircleId: String = null, //基于圈子的算法id
  var algoOption: String = null,//被选择的算法类的全路径名
  var dataSource: String = null,//数据源类型  
  
  var path: String = null//txt源数据路径
){
}

object UserConf{
  
  /*
   * hbase配置信息初始化方法
   */
  def apply(namespace: String, confTable: String, behTable: String, pAttrTable: String, 
      iAttrTable: String, userRegId: String, serviceId: String, sceneId: String,algoId: String, 
      algoUserId: String, algoCircleId: String, algoOption: String, dataSource: String ){
    val user = new UserConf()
    user.namespace = namespace
    user.algoCircleId = algoCircleId
    user.algoId = algoId
    user.algoOption = algoOption
    user.algoUserId = algoUserId
    user.behTable = behTable
    user.confTable = confTable
    user.dataSource = dataSource
    user.iAttrTable = iAttrTable
    user.pAttrTable = pAttrTable
    user.sceneId = sceneId
    user.serviceId = serviceId
    user.userRegId = userRegId
  }
  
  /*
   * txt源数据配置信息初始化方法
   */
  def apply(path: String){
    val user = new UserConf()
    user.path = path
  }
}