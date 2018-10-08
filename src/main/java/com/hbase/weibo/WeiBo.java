package com.hbase.weibo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class WeiBo {

	// 创建conf对象
	private Configuration conf = HBaseConfiguration.create();

	// 命名空间的名称
	private static final String NS_WEIBO = "ns_weibo";

	// 列族info的名称
	private static final byte[] CF_INFO = Bytes.toBytes("info");
	// 列族attends的名称
	private static final byte[] CF_ATTENDS = Bytes.toBytes("attends");
	// 列族fans的名称
	private static final byte[] CF_FANS = Bytes.toBytes("fans");

	// 微博内容表 表名
	private static final byte[] TABLE_CONTENT = Bytes.toBytes(NS_WEIBO + ":content");

	// 微博用户关系表表名
	private static final byte[] TABLE_REALTION = Bytes.toBytes(NS_WEIBO + ":relation");

	// 微博收件箱表表名
	private static final byte[] TABLE_INBOX = Bytes.toBytes(NS_WEIBO + ":inbox");

	/**
	 * 初始化方法 创建命名空间 创建三张表
	 */
	public void init() {
		//TODO 初始化
		initNamespace();
		createTableContent();
		createTableRealtion();
		createTableInbox();
	}

	/**
	 * 初始化命名空间
	 */
	// TODO 初始化命名空间
	public void initNamespace() {
		HBaseAdmin admin = null;
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();

			NamespaceDescriptor ns_weibo = NamespaceDescriptor.create(NS_WEIBO).addConfiguration("creator", "JinJi")
					.addConfiguration("create_time", String.valueOf(System.currentTimeMillis())).build();

			admin.createNamespace(ns_weibo);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭本次HBase的连接，释放资源
			if (admin != null) {
				try {
					admin.close();
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// TODO
	/**
	 * 创建微博内容表 创建微博内容表 TableName:content ColumnFamily:info
	 * ColumnLable:微博的文字信息，微博的图片URL, 微博视频的URl,广告推广信息，活动信息
	 * Value:微博的文字信息，微博的图片URL, 微博视频的URl,广告推广信息，活动信息 举例：message:今天天气不错 Version:1
	 */
	public void createTableContent() {
		HBaseAdmin admin = null;
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();

			// 创建表描述器
			HTableDescriptor contentTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_CONTENT));

			// 创建列描述器
			HColumnDescriptor infoColumnDecriptor = new HColumnDescriptor(CF_INFO);

			// 设置压缩方式
			// infoColumnDecriptor.setCompressionType(Algorithm.SNAPPY);

			// 设置版本确界
			infoColumnDecriptor.setVersions(1, 1);

			contentTableDescriptor.addFamily(infoColumnDecriptor);
			admin.createTable(contentTableDescriptor);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭本次HBase的连接，释放资源
			if (admin != null) {
				try {
					admin.close();
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// TODO
	// 创建用户关系表
	// TableName:relation
	// ColumnFamily: attends , fans
	// ColumnLable:关注的人用户id即，uid(粉丝亦然)
	// Value:用户id
	// Version:1
	public void createTableRealtion() {
		HBaseAdmin admin = null;
		Connection connection = null;

		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();

			// 创建表描述器
			HTableDescriptor relationTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_REALTION));

			// 创建列描述器
			// attends
			HColumnDescriptor attendsColumnDescriptor = new HColumnDescriptor(CF_ATTENDS);

			// 设置版本确界
			attendsColumnDescriptor.setVersions(1, 1);

			// fans// 创建列描述器
			HColumnDescriptor fansColumnDescriptor = new HColumnDescriptor(CF_FANS);

			// 设置版本确界
			fansColumnDescriptor.setVersions(1, 1);

			// 将两个封装好的列族添加到表描述器中
			relationTableDescriptor.addFamily(attendsColumnDescriptor);
			relationTableDescriptor.addFamily(fansColumnDescriptor);

			admin.createTable(relationTableDescriptor);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭本次HBase的连接，释放资源
			if (admin != null) {
				try {
					admin.close();
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	

	// TODO 创建微博收件箱表 
	/**
	 * 创建微博收件箱表 
	 * TableName:inbox ColumnFamily:info ColumnLabel:发布微博的人的用户id Value:
	 * 对应的那个人发布的微博的人用户id Version:多个，比如1000个
	 */
	public void createTableInbox() {
		
		HBaseAdmin admin = null;
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			admin = (HBaseAdmin) connection.getAdmin();

			// 创建表描述器
			HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_INBOX));

			// 创建列描述器
			HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(CF_INFO);

			infoColumnDescriptor.setVersions(1000, 1000);

			inboxTableDescriptor.addFamily(infoColumnDescriptor);

			admin.createTable(inboxTableDescriptor);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭本次HBase的连接，释放资源
			if (admin != null) {
				try {
					admin.close();
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// TODO 发布微博 
	/**
	 * 发布微博 
	 * 1、将发布的微博数据添加到微博内容表中 
	 * 2、将微博rowkey送到发布微博的人的粉丝的收件箱中
	 */
	public void publishContent(String uid, String content) {

		Connection connection = null;

		try {

			connection = ConnectionFactory.createConnection(conf);

			// 1.将发布的微博数据添加到微博内容表中
			Table contentType = connection.getTable(TableName.valueOf(TABLE_CONTENT));

			// 设计rowkey
			String rowKey = uid + "_" + System.currentTimeMillis();

			// 创建当前行的put对象
			Put contentPut = new Put(Bytes.toBytes(rowKey));
			contentPut.addColumn(CF_INFO, Bytes.toBytes("content"), Bytes.toBytes(content));
			contentType.put(contentPut);

			// ----------------------------------------------------------------------------------------------
			// 2.将微博rowkey送到发布微博的人的粉丝的收件箱
			// 2.1线获取到该发布微博人的粉丝
			Table relationTable = connection.getTable(TableName.valueOf(TABLE_REALTION));

			// 2.2取出所有粉丝数据
			Get get = new Get(Bytes.toBytes(uid));
			get.addFamily(CF_FANS);

			// 得到fans列族下所有的数据
			Result result = relationTable.get(get);

			List<byte[]> fansList = new ArrayList<byte[]>();
			Cell[] cells = result.rawCells();

			// 经过该for循环可以得到当前发布微博的人的所有的粉丝uid
			for (Cell cell : cells) {
				fansList.add(CellUtil.cloneQualifier(cell));
			}
			// 如果没有粉丝，则直接return
			if (fansList.size() <= 0)
				return;

			// 2.3开始操作收件箱
			Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
			List<Put> putList = new ArrayList<Put>();

			for (byte[] f : fansList) {
				Put fansPut = new Put(f);
				fansPut.addColumn(CF_INFO, Bytes.toBytes(uid), Bytes.toBytes(rowKey));
				putList.add(fansPut);
			}
			inboxTable.put(putList);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			// 关闭本次HBase的连接，释放资源
			if (connection != null) {
				try {
					connection.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	// TODO 关注用户逻辑
	/**
	 * 关注用户逻辑
	 * a、在微博用户关系表中，对当前主动操作的用户添加新的关注的好友
	 * b、在微博用户关系表中，对被关注的用户添加粉丝（当前操作的用户）
	 * c、当前操作用户的微博收件箱添加所关注的用户发布的微博rowkey
	 */
	public void addAttends(String uid, String... attends) {
		// 参数过滤
		if (attends == null || attends.length <= 0 || uid == null || uid.length() <= 0) {
			return;
		}

		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);

			// 用户关系表操作对象（连接到用户关系表）TABLE_REALTION
			Table relationTable = connection.getTable(TableName.valueOf(TABLE_REALTION));
			List<Put> puts = new ArrayList<Put>();
			// a、在微博用户关系表中，添加新关注的好友
			Put attendPut = new Put(Bytes.toBytes(uid));
			for (String attend : attends) {
				// 为当前用户添加关注的人
				attendPut.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend), Bytes.toBytes(attend));
				// b、为被关注的人，添加粉丝
				Put fansPut = new Put(Bytes.toBytes(attend));
				fansPut.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid), Bytes.toBytes(uid));
				// 将所有关注的人一个一个的添加到puts（List）集合中
				puts.add(fansPut);
			}
			puts.add(attendPut);
			relationTable.put(puts);

			// c.1、微博收件箱添加关注的用户发布的微博内容（content）的rowkey
			Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
			Scan scan = new Scan();
			// 用于存放取出来的关注的人所发布的微博的rowkey
			List<byte[]> rowkeys = new ArrayList<byte[]>();

			for (String attend : attends) {
				// 过滤扫描rowkey，即：前置位匹配被关注的人的uid_
				RowFilter filter = new RowFilter(CompareOp.EQUAL, new SubstringComparator(attend + "_"));
				// 为扫描对象指定过滤规则
				scan.setFilter(filter);
				// 通过扫描对象得到scanner
				ResultScanner result = contentTable.getScanner(scan);
				// 迭代器遍历扫描出来的结果集
				Iterator<Result> iterator = result.iterator();
				while (iterator.hasNext()) {
					// 取出每一个符合扫描结果的那一行数据
					Result r = iterator.next();
					for (Cell cell : r.rawCells()) {
						// 将得到的rowkey放置于集合容器中
						rowkeys.add(CellUtil.cloneRow(cell));
					}

				}
			}

			// c.2、将取出的微博rowkey放置于当前操作的用户的收件箱中
			if (rowkeys.size() <= 0)
				return;
			// 得到微博收件箱表的操作对象
			Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
			// 用于存放多个关注的用户的发布的多条微博rowkey信息
			List<Put> inboxPutList = new ArrayList<Put>();
			for (byte[] rk : rowkeys) {
				Put put = new Put(Bytes.toBytes(uid));
				// uid_timestamp
				String rowKey = Bytes.toString(rk);
				// 截取uid
				String attendUID = rowKey.substring(0, rowKey.indexOf("_"));
				long timestamp = Long.parseLong(rowKey.substring(rowKey.indexOf("_") + 1));
				// 将微博rowkey添加到指定单元格中
				put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attendUID), timestamp, rk);
				inboxPutList.add(put);
			}
			inboxTable.put(inboxPutList);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != connection) {
				try {
					connection.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	// TODO 取消关注（remove) 
	/**
	 * 取消关注（remove) 
	 * a、在微博用户关系表中，对当前主动操作的用户删除对应取关的好友
	 * b、在微博用户关系表中，对被取消关注的人删除粉丝（当前操作人）
	 * c、从收件箱中，删除取关的人的微博的rowkey
	 */
	public void removeAttends(String uid, String... attends) {
		// 过滤数据
		if (uid == null || uid.length() <= 0 || attends == null || attends.length <= 0)
			return;

		try {
			Connection connection = ConnectionFactory.createConnection(conf);

			// a、在微博用户关系表中，删除已关注的好友
			Table relationTable = connection.getTable(TableName.valueOf(TABLE_REALTION));
			// 待删除的用户关系表中的所有数据
			List<Delete> deleteList = new ArrayList<Delete>();
			// 当前取关操作者的uid对应的Delete对象
			Delete attendDelete = new Delete(Bytes.toBytes(uid));
			// 遍历取关，同时每次取关都要将被取关的人的粉丝-1
			for (String attend : attends) {
				attendDelete.addColumn(Bytes.toBytes("attends"), Bytes.toBytes(attend));
				// b、在微博用户关系表中，对被取消关注的人删除粉丝（当前操作人）
				Delete fansDelete = new Delete(Bytes.toBytes(attend));
				fansDelete.addColumn(Bytes.toBytes("fans"), Bytes.toBytes(uid));
				deleteList.add(fansDelete);
			}

			deleteList.add(attendDelete);
			relationTable.delete(deleteList);

			// c、删除取关的人的微博rowkey 从 收件箱表中
			Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));

			Delete inboxDelete = new Delete(Bytes.toBytes(uid));
			for (String attend : attends) {
				inboxDelete.addColumn(Bytes.toBytes("info"), Bytes.toBytes(attend));
			}
			inboxTable.delete(inboxDelete);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	// TODO
	/**
	 * 获取微博实际内容
	 * a、从微博收件箱中获取所有关注的人的发布的微博的rowkey
	 * b、根据得到的rowkey去微博内容表中得到数据
	 * c、将得到的数据封装到Message对象中
	 */
	public List<Message> getAttendsContent(String uid){
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
			
			Table inboxTable = connection.getTable(TableName.valueOf(TABLE_INBOX));
			//a、从收件箱中取得微博rowKey
			Get get = new Get(Bytes.toBytes(uid));
			//设置最大版本号
			get.setMaxVersions(5);
			List<byte[]> rowkeys = new ArrayList<byte[]>();
			Result result = inboxTable.get(get);
			for(Cell cell : result.rawCells()){
				rowkeys.add(CellUtil.cloneValue(cell));
			}
			//b、根据取出的所有rowkey去微博内容表中检索数据
			Table contentTable = connection.getTable(TableName.valueOf(TABLE_CONTENT));
			List<Get> gets = new ArrayList<Get>();
			//根据rowkey取出对应微博的具体内容
			for(byte[] rk : rowkeys){
				Get g = new Get(rk);
				gets.add(g);
			}
			//得到所有的微博内容的result对象
			Result[] results = contentTable.get(gets);
			//将每一条微博内容都封装为消息对象
			List<Message> messages = new ArrayList<Message>();
			for(Result res : results){
				for(Cell cell : res.rawCells()){
					Message message = new Message();
					String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
					String userid = rowKey.substring(0, rowKey.indexOf("_"));
					String timestamp = rowKey.substring(rowKey.indexOf("_") + 1);
					String content = Bytes.toString(CellUtil.cloneValue(cell));
					message.setContent(content);
					message.setTimestamp(timestamp);
					message.setUid(userid);
					messages.add(message);
				}
			}
			return messages;
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				connection.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	// TODO
	/**
	 * 发布微博内容
	 * 添加关注
	 * 取消关注
	 * 展示内容
	 */
	public void testPublishContent(WeiBo wb){
		wb.publishContent("0001", "今天买了一包空气，送了点薯片，非常开心！！");
		wb.publishContent("0001", "今天天气不错。");
	}

	public void testAddAttend(WeiBo wb){
		wb.publishContent("0008", "准备下课！");
		wb.publishContent("0009", "准备关机！");
		wb.addAttends("0001", "0008", "0009");
	}

	public void testRemoveAttend(WeiBo wb){
		wb.removeAttends("0001", "0008");
	}

	public void testShowMessage(WeiBo wb){
		List<Message> messages = wb.getAttendsContent("0001");
		for(Message message : messages){
			System.out.println(message);
		}
	}

	// TODO
	public static void main(String[] args) {
		WeiBo weibo = new WeiBo();
		//weibo.init();	
		weibo.testPublishContent(weibo);
		weibo.testAddAttend(weibo);
		weibo.testShowMessage(weibo);
		//weibo.testRemoveAttend(weibo);
		weibo.testShowMessage(weibo);
	}


}
