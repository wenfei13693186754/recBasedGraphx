package com.wdcloud.graphx.javaUtil;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *获取系统当前时间，减去用户传来的时间，得到时间差，用来表示用户之间最近一次交互距 *离现在的时间
 */
public class TimeOperate implements Serializable{
	
	//2.获取当前系统时间  
	private static final Date currentData = new Date();
	/**
	 * 将传进来的yyyy-MM-dd HH:mm:ss类型的时间格式转化为时间戳格式15812272063
	 * @param timeStr 传进来的yyyy-MM-dd HH:mm:ss格式的数据
	 * @return timeStr对应的时间戳
	 */
	public Long DealTime(String timeStr) {
		//1.获取用户与好友之间最近一次交互的时间，因为传过来的是字符串，所以这里将字符串转化为Data
		
		//String dataFormat = "yyyy-MM-dd HH:mm:ss";
		String dataFormat = "yyyyMMddHHmmss";
		SimpleDateFormat sdf = new SimpleDateFormat(dataFormat);
		Date data = null;
		try {
			data = sdf.parse(timeStr);
			//System.out.println("传来时间是："+data);
		} catch (ParseException e) {
			System.out.println("时间格式不匹配:"+timeStr+",请输入yyyy-MM-dd HH:mm:ss格式时间");
			e.printStackTrace();
		}
		//System.out.println("当前时间是："+currentData);
		//3.计算时间差
		long time = data.getTime();
		return time;

	}
	public static void main(String[] args) {
		TimeOperate timeUtil = new TimeOperate();
		//Long a = timeUtil.DealTime("2016-09-06 11:08:08");
		Long a = timeUtil.DealTime("20160622041319");
		System.out.println(a);
	}
}

