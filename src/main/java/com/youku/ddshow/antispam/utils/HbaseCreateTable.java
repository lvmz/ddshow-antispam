package com.youku.ddshow.antispam.utils;

import com.youku.ddshow.antispam.model.PropertiesType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HbaseCreateTable {
	private Configuration conf = null;

	private static final Logger LOG = LoggerFactory.getLogger(HbaseCreateTable.class);

	public HbaseCreateTable(PropertiesType propertiesType) {
		conf = HBaseConfiguration.create();
		PropertiesUtil properties = new PropertiesUtil(propertiesType.getValue());
		conf.set("hbase.zookeeper.quorum", properties.getValue("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", properties.getValue("hbase.zookeeper.property.clientPort"));
	}


	/**
	 * 创建表
	 * 
	 * @param tableName
	 *            表名
	 * @param family
	 *            列名
	 * @throws Exception
	 */
	public void creatTable(String tableName, String[] family, byte[][] regions) throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor desc = new HTableDescriptor(tableName);
		for (int i = 0; i < family.length; i++) {
			HColumnDescriptor columnDesc = new HColumnDescriptor(family[i]);
			desc.addFamily(columnDesc);
			
		}
		if (admin.tableExists(tableName)) {
			System.out.println("table Exists!");
			System.exit(0);
		} else {
			admin.createTable(desc, regions);
			System.out.println("create table Success!");
		}
	}
	
	public void dropTable(String table) {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(table);
			admin.deleteTable(table);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws Exception {
		//System.setProperty("hadoop.home.dir", "D:\\develop\\soft\\hadoop-2.5.2");
		HbaseCreateTable util = new HbaseCreateTable(PropertiesType.DDSHOW_HASE_TEST);
//		String[] columns = new String[] { "stat_date","appid","fk_user","user_name","ip","room_id","content","user_level","time" };
//		util.creatTable("lf_t_detail_user_chat_content_room", columns);
		
		
//		String[] columns = new String[] {"appid","stat_date","min","roomid","total_people_num","total_num","total_amount","total_money_num","show_people_num","show_num","show_amount","show_money_num","epb_people_num","epb_num","epb_amount","epb_money_num"};
//		util.creatTable("lf_t_view_user_cost_stat", columns);
		
		byte[][] regions = new byte[][] {
				  Bytes.toBytes("00"),    
				  Bytes.toBytes("01"),  
				  Bytes.toBytes("02"),  
				  Bytes.toBytes("03"),  
				  Bytes.toBytes("04"),  
				  Bytes.toBytes("05"),  
				  Bytes.toBytes("06"),
				  Bytes.toBytes("07"),  
				  Bytes.toBytes("08"),  
				  Bytes.toBytes("09")    
				}; 
		
		util.dropTable("lf_t_view_hbase_room_stat");
		
		String[] columns = new String[] {"roomid","stat_date", "base_info", "popularNum"};
		util.creatTable("lf_t_view_hbase_room_stat", columns, regions);

	}
}
