package com.wdcloud.graphx.pojo

object LogTypeWithNum {
	/** 创建[item] */
	val CREATE = "01";
	/** 分享[share] */
	val SHARE = "02";
	/** 收藏[collect] */
	val COLLECT = "03";
	/** 取消收藏[uncollect] */
	val UNCOLLECT = "04";
	/** 点赞[like] */
	val LIKE = "05";
	/** 取消点赞[unlike] */
	val UNLIKE = "06";
	/** 评论[comment] */
	val COMMENT = "07";
	/** 回复[reply] */
	val REPLY = "08";
	/** 关注[follow] */
	val FOLLOW = "09";
	/** 取消关注[unfollow] */
	val UNFOLLOW = "10";
	/** 浏览[view] */
	val VIEW = "11";
	/** 加入[join] */
	val JOIN = "12";
	/** 退出 */
	val OUT = "13";
	/** 加为好友 */
	val ADDFriend = "14";
	/** 解除好友 */
	val DELETEFriend = "15";
	/**评价 */
	val RATE = "16";
}



/**
 * <ol>
 * <li>创建[item]</li>
 * <li>分享[share]</li>
 * <li>收藏[collect]</li>
 * <li>取消收藏[uncollect]</li>
 * <li>点赞[like]</li>
 * <li>取消点赞[unlike]</li>
 * <li>评论[comment]</li>
 * <li>回复[reply]</li>
 * <li>评分[grade]</li>
 * <li>关注[follow]</li>
 * <li>取消关注[unfollow]</li>
 * <li>浏览[view]</li>
 * <li>加入[join]</li>
 * </ol>
 */
object LogType {
	/** 创建[item] */
	val ITEM = "item";
	/** 分享[share] */
	val SHARE = "share";
	/** 收藏[collect] */
	val COLLECT = "collect";
	/** 取消收藏[uncollect] */
	val UNCOLLECT = "uncollect";
	/** 点赞[like] */
	val LIKE = "like";
	/** 取消点赞[unlike] */
	val UNLIKE = "unlike";
	/** 评论[comment] */
	val COMMENT = "comment";
	/** 回复[reply] */
	val REPLY = "reply";
	/** 评分[grade] */
	val GRADE = "grade";
	/** 关注[follow] */
	val FOLLOW = "follow";
	/** 取消关注[unfollow] */
	val UNFOLLOW = "unfollow";
	/** 浏览[view] */
	val VIEW = "view";
	/** 加入[join] */
	val JOIN = "join";
}

