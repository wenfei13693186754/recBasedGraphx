package com.wdcloud.graphx.pojo

/**
 * 一些配置信息
 */
case class Algo(
    userRegId: String, //用户注册使用的id
    serviceId: String, //新建业务id
    sceneId: String, //新建场景id
    
    //新建算法模板
    algoId: String,//算法id 
    namespace: String, //命名空间
    confTable: String, //用户配置信息表名
    behTable: String, //用户行为表名
    algoUserId: String, //基于二度好友的算法id
    algoCircleId: String, //基于圈子的算法id
    algoOption: String,//被选择的算法类的全路径名
    dataSource: String)//数据源格式（txt/hbase等）