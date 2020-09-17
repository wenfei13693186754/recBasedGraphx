package com.wdcloud.graphx.javaUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * 
 * @author Administrator
 *
 */
public class CreateData {

	public static void main(String[] args) throws Exception {
		// 产生每个用户和好友之间的关系,用于协同过滤中数据的产生
		// createRelationBetweenFriends();
		// 产品1 2这样的边关系
		// createDataFor1To2();

		String path = "E:\\spark\\Spark-GraphX\\data\\recItemBasedCircleData3\\relInGood1";
		// 为图计算用到的attr文件生成测试数据
		// 分别产生person、circle、book类型数据
		// createAttrDataForGraphX(path, 100000);

		// 为图计算产生edges数据
		// person_1 person_2|100 0 0 0 0 0|2016-09-06 11:08:08
		// createEdgesDateForGraphX1(path, 500000);

		// 用来生成向hbase中写入的测试数据
		// 数据格式是：person_4 person person_5 person 1 0 0 0 0 0 0 2016-09-06
		// 11:08:08
		createEdgesDateForGraphX2();

		// 为图计算产生feat数据
		// person_1 1 1 1 1 1 1 1 1 1 1
		// createFeatDateForGraphX(path,10000);

		// 为图产生.circle文件
		// 123456|person_1 person_2....
		// createCircleDataForGraphX();

		// String path1 =
		// "E:\\spark\\Spark-GraphX\\data\\hashingOfGuavaTest\\hashingData";
		// createDataLikeX23asXds23(20, 10000000, path1,"item");
	}

	// 生成随机数字和字母,
	private static void createDataLikeX23asXds23(int length, int data,
			String path, String name) throws Exception {

		Random random = new Random();

		// 参数length，表示生成几位随机数
		FileOutputStream fs = new FileOutputStream(new File(path));
		PrintStream p = new PrintStream(fs);
		for (int i = 1; i <= data; i++) {
			String val = "";
			for (int j = 0; j < length; j++) {
				String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
				// 输出字母还是数字
				if ("char".equalsIgnoreCase(charOrNum)) {
					// 输出是大写字母还是小写字母
					int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
					val += (char) (random.nextInt(26) + temp);
				} else if ("num".equalsIgnoreCase(charOrNum)) {
					val += String.valueOf(random.nextInt(10));
				}
			}

			p.println(val);
		}
		p.close();
	}

	private static void createDataFor1To2() throws FileNotFoundException {
		Random rand = new Random();
		FileOutputStream fs = new FileOutputStream(
				new File(
						"E:\\spark\\Spark-GraphX\\data\\recItemBasedCircleAndSimUser\\1.txt"));
		PrintStream p = new PrintStream(fs);

		// 产生十个圈子，每个圈子里边有100个人
		for (int i = 0; i < 100; i++) {
			int l1 = rand.nextInt(1000);
			int l2 = rand.nextInt(1000);
			p.println(l1 + " " + l2);
		}
		p.close();
	}

	private static void createCircleDataForGraphX()
			throws FileNotFoundException {
		Random rand = new Random();
		FileOutputStream fs = new FileOutputStream(new File(
				"E:\\spark\\Spark-GraphX\\data\\circleData\\userCircle.circle"));
		PrintStream p = new PrintStream(fs);

		// 产生十个圈子，每个圈子里边有100个人
		for (int i = 0; i < 10; i++) {
			int circleId = 0;
			while (circleId < 100000) {
				circleId = (int) (Math.random() * 1000000);
			}
			p.print(circleId + "|");
			for (int j = 1; j <= 10; j++) {
				p.print("person_" + rand.nextInt(30) + " ");
			}
			p.println();
		}
		p.close();
	}

	// 为图计算产生feat数据
	// person_1 1 1 1 1 1 1 1 1 1 1
	private static void createFeatDateForGraphX(String path, int data)
			throws FileNotFoundException {
		Random rand = new Random();
		FileOutputStream fs = new FileOutputStream(new File(path + ".feat"));
		PrintStream p = new PrintStream(fs);
		for (int i = 1; i <= data; i++) {
			int p1 = i;
			int f1 = rand.nextInt(2);
			int f2 = rand.nextInt(2);
			int f3 = rand.nextInt(2);
			int f4 = rand.nextInt(2);
			int f5 = rand.nextInt(2);
			int f6 = rand.nextInt(2);
			int f7 = rand.nextInt(2);
			int f8 = rand.nextInt(2);
			int f9 = rand.nextInt(2);
			int f10 = rand.nextInt(2);
			p.println("person_" + p1 + " " + f1 + " " + f2 + " " + f3 + " "
					+ f4 + " " + f5 + " " + f6 + " " + f7 + " " + f8 + " " + f9
					+ " " + f10);
		}

		for (int i = 1; i <= data; i++) {
			int p1 = i;
			int f1 = rand.nextInt(2);
			int f2 = rand.nextInt(2);
			int f3 = rand.nextInt(2);
			int f4 = rand.nextInt(2);
			int f5 = rand.nextInt(2);
			int f6 = rand.nextInt(2);
			int f7 = rand.nextInt(2);
			int f8 = rand.nextInt(2);
			int f9 = rand.nextInt(2);
			int f10 = rand.nextInt(2);
			p.println("circle_" + p1 + " " + f1 + " " + f2 + " " + f3 + " "
					+ f4 + " " + f5 + " " + f6 + " " + f7 + " " + f8 + " " + f9
					+ " " + f10);
		}

		for (int i = 1; i <= data; i++) {
			int p1 = i;
			int f1 = rand.nextInt(2);
			int f2 = rand.nextInt(2);
			int f3 = rand.nextInt(2);
			int f4 = rand.nextInt(2);
			int f5 = rand.nextInt(2);
			int f6 = rand.nextInt(2);
			int f7 = rand.nextInt(2);
			int f8 = rand.nextInt(2);
			int f9 = rand.nextInt(2);
			int f10 = rand.nextInt(2);
			p.println("book_" + p1 + " " + f1 + " " + f2 + " " + f3 + " " + f4
					+ " " + f5 + " " + f6 + " " + f7 + " " + f8 + " " + f9
					+ " " + f10);
		}
		p.close();
	}

	// 为图计算产生edges数据
	// person_1 person_2|100 0 0 0 0 0|2016-09-06 11:08:08
	private static void createEdgesDateForGraphX1(String path, int data)
			throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(new File(path + ".edges"));
		PrintStream p = new PrintStream(fs);
		for (int i = 0; i < data; i++) {
			int p1 = (int) (Math.random() * 10000);
			int p2 = (int) (Math.random() * 10000);
			int at1 = (int) (Math.random() * 10);
			int at2 = (int) (Math.random() * 10);
			int at3 = (int) (Math.random() * 10);
			int at4 = (int) (Math.random() * 10);
			int at5 = (int) (Math.random() * 10);
			int at6 = (int) (Math.random() * 100);
			if (p1 == p2) {
				i--;
				continue;
			}
			// 用来产生随机时间
			Date date = randomDate("2016-01-06 11:08:08", "2016-09-06 11:08:08");
			SimpleDateFormat format = new SimpleDateFormat(
					"yyyy-MM-dd hh:mm:ss");
			String strDate = format.format(date);
			Random rand = new Random();
			int num = rand.nextInt(3);

			switch (num) {
			case 0:
				p.println("person_" + p1 + " " + "person_" + p2 + "|" + at1
						+ " " + at2 + " " + at3 + " " + at4 + " " + at5 + " "
						+ at6 + "|" + strDate);
			case 1:
				p.println("person_" + p1 + " " + "circle_" + p2 + "|" + at1
						+ " " + at2 + " " + at3 + " " + at4 + " " + at5 + " "
						+ at6 + "|" + strDate);
			default:
				p.println("person_" + p1 + " " + "book_" + p2 + "|" + at1 + " "
						+ at2 + " " + at3 + " " + at4 + " " + at5 + " " + at6
						+ "|" + strDate);
			}
		}
		p.close();
	}

	/*
	 * 为图计算产生edges数据,数据格式如下： ACCOUNT USER_ID ITEM_ID CATEGORY ACTION
	 * BHV_DATETIME CREATETIME january person_1 person_2 03 12 1483948828680
	 * 1483948828680 生成规则： 顶点id：
	 * 顶点业务id由UUID生成，共32位，由此生成的内部顶点id是经过hash后的long类型id。 businessId--innerId
	 * CATEGORY物品类别： 由01--10共10个字符串组成，分别对应不同的物品类别 (考虑不使用字符串，因为字符串所占用的内存大于基本类型的)
	 * ACTION行为类型： 由01--16共16个字符串组成，分别对应不同的行为类型。 (考虑不使用字符串，因为字符串所占用的内存大于基本类型的)
	 * BHV_DATETIME行为发生时间： 时间戳形式，造的数据中它们的取值范围在2000年1月1日到现在； CREATETIME信息创建时间：
	 * 造数据的时间，就是当前时间。
	 * 
	 * 数据量： 顶点1000 用户900个 物品10种，共10个
	 * 
	 * 边10000条 用户和用户之间、用户和物品之间的关系都是随机的。 每个用户随机产生10--100个好友 1--100个物品
	 */
	private static void createEdgesDateForGraphX2()
			throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(new File(
				"E:\\推荐系统\\graphEngine\\测试数据信息\\edges6800.txt"));
		PrintStream p = new PrintStream(fs);

		Random rand = new Random();

		// 先获取1000个顶点的UUID
		String[] userPointIds = getUUID(6800);
		String[] itemPointIds = getUUID(500);
		// 时间格式的转化 january person_1 person_2 03 12 1483948828680 1483948828680
		int num = 0;
		for (int i = 0; i < 6800; i++) {
			num++;
			// 每个用户对应1--200个好友数据的生成
			int fNum = (int) (Math.random() * 400) + 1;
			Set<String> set = new HashSet<String>(fNum);
			while (set.size() < fNum) {// 好友id不能重复，所以使用set集合
				int index = (int) (Math.random() * 6800) + 1;
				String fId = userPointIds[index - 1];
				if (set.contains(userPointIds[i])) {
				}
				set.add(fId);
			}

			// 将用户和好友关系数据写出
			for (String fId : set) {
				// 用来产生随机时间
				Date date = randomDate("2000-01-01 00:00:00",
						"2017-03-20 11:08:08");
				long time = date.getTime();

				// 产生行为类型
				int bhvNum = (int) (Math.random() * 16) + 1;
				String bhvType;
				if (bhvNum < 10) {
					bhvType = "0" + bhvNum;
				} else {
					bhvType = bhvNum + "";
				}

				p.println("january " + userPointIds[i] + " " + fId + " 01 "	+ bhvType + " " + time + " "+ System.currentTimeMillis());
			}
			/*
			 * 生成用户和物品关系并写出 1--1000个物品
			 */
			int iNum = (int) (Math.random() * 500) + 1;
			Set<String> setItem = new HashSet<String>();
			while (setItem.size() < iNum) {// 同一个用户的物品id不能重复，所以使用set集合
				String itemType = (int) (Math.random() * 500) + 1+"";
				int itemNum = (int) (Math.random() * 500);
				String itemId = itemPointIds[itemNum];
				setItem.add(itemId+" "+itemType);
			}
			
			for (String item : setItem) {
				String[] itemInfo = item.split(" ");
				String itemId = itemInfo[0];
				//产生物品类型
				String itemType = itemInfo[1];
				if(Integer.parseInt(itemType)<10){
					itemType = "0"+itemType;
				}
				// 产生行为类型
				int bhvNum = (int) (Math.random() * 16) + 1;
				String bhvType;
				if (bhvNum < 10) {
					bhvType = "0" + bhvNum;
				} else {
					bhvType = bhvNum + "";
				}
				
				// 用来产生随机时间  january person_1 person_2 03 12 1483948828680 1483948828680
				//"january " + userPointIds[i] + " " + fId + " 01 "	+ bhvType + " " + time + " "+ System.currentTimeMillis()
				Date date = randomDate("2000-01-01 00:00:00",
						"2017-03-20 11:08:08");
				long time = date.getTime();
				p.println("january " + userPointIds[i] + " " + itemId + " " + itemType+" "+ bhvType +" "+time+" "+System.currentTimeMillis());

			}
			System.out.println("第"+num+"个用户");
		}
	}

	// 为图计算用到物品的attr文件生成测试数据
	private static void createAttrDataForGraphX(String path, int data)
			throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(
				new File(
						"E:\\spark\\Spark-GraphX\\data\\recFriendsItemAndCircle\\relInGood1.attr"));
		PrintStream p = new PrintStream(fs);
		// int avgData = data/3;

		for (int i = 1; i <= 10000; i++) {// person的数据格式：person_1 person girl 25
											// beijing rose 1 3
			p.println("person_" + i + "|"
					+ "person girl 25 beijing rose 1 3 12312");
		}

		for (int i = 1; i <= 100; i++) { // circle_2 circle friends 10
											// beijing play 1 3
			p.println("circle_" + i + "|"
					+ "circle friends 10 beijing play 1 3 123123");
		}
		for (int i = 1; i <= 49; i++) {// book1_1 book java 10 河北 java编程思想 1 3
			for (int j = 1; j <= 100; j++) {
				p.println("book" + i + "_" + j + "|" + "book" + i
						+ " java 10 河北 java编程思想 1 3 123123");
			}
		}
		p.close();
	}

	// 产生每个用户和好友之间的关系,用于协同过滤中数据的产生
	private static void createRelationBetweenFriends()
			throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(new File(
				"E:\\Spark-MLlib\\data\\user6.txt"));
		PrintStream p = new PrintStream(fs);
		// p.println(100);
		// p.close();
		for (int i = 0; i < 10000; i++) {
			// for(int j=0;j<1;j++){
			// if(j!=i){
			double fdata = (Double) (Math.random() * 0.9);
			double num = (Double) (Math.random() * 0.9);
			double cz = (Double) (Math.random() * 0.9);
			double other = (Double) (Math.random() * 0.9);
			p.println(i + "|" + fdata + " " + num + " " + cz + " " + other);
			// }
			// }
		}
		p.close();
	}

	// ****************生成随机时间**************************************************************************
	private static Date randomDate(String beginDate, String endDate) {

		try {

			SimpleDateFormat format = new SimpleDateFormat(
					"yyyy-MM-dd hh:mm:ss");

			Date start = format.parse(beginDate);// 构造开始日期

			Date end = format.parse(endDate);// 构造结束日期

			// getTime()表示返回自 1970 年 1 月 1 日 00:00:00 GMT 以来此 Date 对象表示的毫秒数。

			if (start.getTime() >= end.getTime()) {

				return null;

			}

			long date = random(start.getTime(), end.getTime());

			return new Date(date);

		} catch (Exception e) {

			e.printStackTrace();

		}

		return null;

	}

	private static long random(long begin, long end) {

		long rtn = begin + (long) (Math.random() * (end - begin));

		// 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值

		if (rtn == begin || rtn == end) {

			return random(begin, end);

		}

		return rtn;

	}

	/**
	 * 获得一个UUID
	 * 
	 * @return String UUID
	 */
	public static String getUUID() {
		String s = UUID.randomUUID().toString();
		// 去掉“-”符号
		return s.substring(0, 8) + s.substring(9, 13) + s.substring(14, 18)
				+ s.substring(19, 23) + s.substring(24);
	}

	/**
	 * 获得指定数目的UUID
	 * 
	 * @param number
	 *            int 需要获得的UUID数量
	 * @return String[] UUID数组
	 */
	public static String[] getUUID(int number) {
		if (number < 1) {
			return null;
		}
		String[] ss = new String[number];
		for (int i = 0; i < number; i++) {
			ss[i] = getUUID();
		}
		return ss;
	}

}
