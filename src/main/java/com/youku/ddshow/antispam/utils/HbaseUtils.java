package com.youku.ddshow.antispam.utils;

import com.alibaba.fastjson.JSON;
import com.youku.ddshow.antispam.model.PropertiesType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcatenatedLists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;
import scala.Tuple4;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class HbaseUtils {
	private Configuration conf = null;
	HTable table = null;
	private String tableName;

	private static final Logger LOG = LoggerFactory.getLogger(HbaseUtils.class);

	public HbaseUtils(PropertiesType propertiesType, String tableName) {
		conf = HBaseConfiguration.create();
		PropertiesUtil properties = new PropertiesUtil(propertiesType.getValue());
		conf.set("hbase.zookeeper.quorum", properties.getValue("hbase.zookeeper.quorum"));
		conf.set("hbase.zookeeper.property.clientPort", properties.getValue("hbase.zookeeper.property.clientPort"));


		try {
			table = new HTable(conf, Bytes.toBytes(tableName));
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}

		this.tableName = tableName;
	}

	/**
	 * 添加数据
	 * 
	 * @param rowKey
	 * @param column
	 * @param value
	 */
	public void addData(String rowKey, String column, Object value) {
		try {
			Put put = new Put(Bytes.toBytes(rowKey));
			put.add(Bytes.toBytes(column), Bytes.toBytes(column), Bytes.toBytes(value.toString()));
			table.put(put);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}
	
	/**
	 * 添加数据
	 * @param  put
	 */
	public void addData(Put put) {
		try {
			table.put(put);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	/**
	 * 批量添加数据
	 * 
	 * @param list
	 */
	public void addDataBatch(List<Put> list) {
		try {
			table.put(list);
		} catch (RetriesExhaustedWithDetailsException e) {
			LOG.error(e.getMessage());
		} catch (InterruptedIOException e) {
			LOG.error(e.getMessage());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
	public void creatTable(String tableName, String[] family) throws Exception {
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
			admin.createTable(desc);
			System.out.println("create table Success!");
		}
	}

	public void dropTable() {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 *
	 * @param rowkeyList
     */
     public void deleteRow(List<String> rowkeyList)  {  
        try {  
            List<Delete> list = new ArrayList<Delete>(); 
            for(String rowkey : rowkeyList) {
            	Delete d1 = new Delete(rowkey.getBytes());
                list.add(d1);
            }
            table.delete(list);  
            System.out.println("删除行成功!");
        } catch (IOException e) {  
            e.printStackTrace();  
        }
    }  

	/**
	 * 查询全部
	 */
	public void queryAll() {
		Scan scan = new Scan();
		try {
			ResultScanner results = table.getScanner(scan);
			for (Result result : results) {
				int i = 0;
				for (KeyValue rowKV : result.list()) {
					
					System.out.print(" " + new String(rowKV.getQualifier()) + " ");
					System.out.print(":" + new String(rowKV.getValue()));
					
				}

				System.out.println();
			}
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}

	}

	/**
	 * 按某字段查询 column = value 的数据
	 * 
	 * @param queryColumn
	 *            要查询的列名
	 * @param value
	 *            过滤条件值
	 * @param columns
	 *            返回的列名集合
	 */
	public ResultScanner queryBySingleColumn(String queryColumn, String value, String[] columns) {
		if (columns == null || queryColumn == null || value == null) {
			return null;
		}

		try {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(queryColumn), Bytes.toBytes(queryColumn), CompareOp.EQUAL, new SubstringComparator(value));
			Scan scan = new Scan();

			for (String columnName : columns) {
				scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
			}

			scan.setFilter(filter);
			return table.getScanner(scan);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

		return null;
	}
	
	/**
	 * 根据对比条件，按某字段查询 column = value 的数据
	 * 
	 * @param queryColumn
	 *            要查询的列名
	 * @param value
	 *            过滤条件值
	 * @param columns
	 *            返回的列名集合
	 * @param op 操作符号 CompareOp.EQUAL 代表等于,CompareOp.LESS代表小于
	 */
	public ResultScanner queryBySingleColumnOnCompareOp(String queryColumn, String value, String[] columns, CompareOp op) {
		if (columns == null || queryColumn == null || value == null) {
			return null;
		}

		try {
			SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(queryColumn), Bytes.toBytes(queryColumn), op, new SubstringComparator(value));
			Scan scan = new Scan();

			for (String columnName : columns) {
				scan.addColumn(Bytes.toBytes(columnName), Bytes.toBytes(columnName));
			}

			scan.setFilter(filter);
			return table.getScanner(scan);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

		return null;
	}

    /**
     * 在指定的条件下，按某一字段聚合
     * @param paramMap 参数条件
     * @param dimensionColumns 维度
     * @param aggregateColumn 聚合字段
     * @return 返回map，key 为dimensionColumns 维度相对应的数据，value 为aggregateColumn 字段对应的值
     */
	public Map<String, Long> aggregateBySingleColumn(Map<String, String> paramMap, String[] dimensionColumns, String aggregateColumn) {
		if (dimensionColumns == null || dimensionColumns.length == 0 || paramMap == null || aggregateColumn == null || aggregateColumn.equals("")) {
			return null;
		}

		Map<String, Long> map = null;
		try {
			FilterList filterList = new FilterList();
			Scan scan = new Scan();
			//添加过滤条件
			for (String paramKey : paramMap.keySet()) {
				SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(paramKey), Bytes.toBytes(paramKey), CompareOp.EQUAL, new SubstringComparator(paramMap.get(paramKey)));
				filterList.addFilter(filter);
			}
			scan.setFilter(filterList);

			//要展现的列
			for (String column : dimensionColumns) {
				scan.addColumn(Bytes.toBytes(column), Bytes.toBytes(column));
			}
			scan.addColumn(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn));

			ResultScanner results = table.getScanner(scan);

			//将查询结果放入map 中
			map = new ConcurrentHashMap<String, Long>();
			for (Result result : results) {
//				String dimensionKey = "";
				StringBuilder dimensionKey = new StringBuilder();
				//取值
				String value = new String(result.getValue(Bytes.toBytes(aggregateColumn), Bytes.toBytes(aggregateColumn)));
				Long aggregateValue = value == null? 0 : Long.parseLong(value);
				
				//拼接Key
				for (String column : dimensionColumns) {
					dimensionKey.append("\t" + new String(result.getValue(Bytes.toBytes(column), Bytes.toBytes(column))));
				}
				dimensionKey = dimensionKey.deleteCharAt(0);

				if(map.containsKey(dimensionKey)) {
					map.put(dimensionKey.toString(), map.get(dimensionKey.toString()) + aggregateValue);
				} else {
					map.put(dimensionKey.toString(), aggregateValue);
				}
			}
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

		return map;
	}
	
	public static void println(ResultScanner results) {
		for (Result result : results) {
			int i = 0;
			for (KeyValue rowKV : result.list()) {
				if (i++ == 0) {
					System.out.print("rowkey:" + new String(rowKV.getRow()) + " ");
				}
				System.out.print(" " + new String(rowKV.getQualifier()) + " ");
				System.out.print(":" + new String(rowKV.getValue()));
				
			}

			System.out.println();
		}
		
		results.close();
	}
	
	public ResultScanner queryByRowKey(String start, String end) {
		try {
			Scan scan = new Scan();
			scan.setStartRow(Bytes.toBytes(start));
			scan.setStopRow(Bytes.toBytes(end));
			return table.getScanner(scan);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

		return null;
	}
	
	public ResultScanner queryByTimeStamp(String start, String end) {
        Date startDate = CalendarUtil.dateStringParse(start, "yyyy-MM-dd HH:mm:ss");
        Date endDate = CalendarUtil.dateStringParse(end, "yyyy-MM-dd HH:mm:ss");
		try {
			Scan scan = new Scan();
			scan.setTimeRange(startDate.getTime(), endDate.getTime());
			return table.getScanner(scan);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}

		return null;
	}
	/**
	 * 查找一行记录
	 */
	public  Tuple3<String,String,Map<String,String>> getOneRecord (String rowKey,String family) throws IOException{
		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);

       // String family = null;
		Map<String,String> kvMap = new TreeMap<>();
		for(KeyValue kv : rs.raw()){
			if(family.equals(new String(kv.getFamily())))
			{
				kvMap.put(new String(kv.getQualifier()),new String(kv.getValue()));
			}
		}
		return new Tuple3<String,String,Map<String,String>>(rowKey,family,kvMap);
	}



	/**
	 * 查找rank 值
	 */
	public  Integer getRank (String rowKey,String family) throws IOException{
		Tuple3<String,String,Map<String,String>> tuple3 =  getOneRecord(rowKey,family);
		Map map =  tuple3._3();
		if( map.get("rank")!=null)
		{
			return  Integer.parseInt(map.get("rank").toString());
		}else
		{
			return null;
		}
	}

	public  Map  getRanks(List<String> rowkeyList,String family,String qualifier ) throws IOException{
           List<Get> getList = new ArrayList<Get>();
		   for(String rowkey: rowkeyList)
			 {
				 Get get = new Get(rowkey.getBytes());
				 getList.add(get);
			 }
		 Result[] results =  table.get(getList);
		Map<String,String> kvMap = new TreeMap<>();
		 for(Result rs:results )
		 {
			 for(KeyValue kv : rs.raw()){
				 if(family.equals(new String(kv.getFamily()))&&qualifier.equals(new String(kv.getQualifier())))
				 {
					  kvMap.put(new String(kv.getRow()).split("_")[1],new String(kv.getValue()));
				 }
			 }
		 }
		return kvMap;
	}



	/**
	 * 普通生成rowkey
	 * @param roomid
     * @return
     */
	public static  String createRowKey(Integer roomid)
	{
		if(roomid<0||roomid==null) return  null;
		Integer mod = roomid % 10;
		String modStr = mod + "";
		if(mod < 10) {
			modStr = "0" + mod;
		}
		String statDate = CalendarUtil.getToday();
		String rowkey = modStr + "_" + roomid + "_" + statDate;
		return rowkey;
	}

	/**
	 * 为rank值生成rowkey 由于rank值只有昨天的数据，所以日期沿用昨天的
	 * @param roomid
	 * @return
     */
	public static String createRowKeyForRank(Integer roomid)
	{
		if(roomid<0||roomid==null) return  null;
		Integer mod = roomid % 10;
		String modStr = mod + "";
		if(mod < 10) {
			modStr = "0" + mod;
		}
		String statDate = CalendarUtil.getYesterday();
		String rowkey = modStr + "_" + roomid + "_" + statDate;
		return rowkey;
	}
	/**
	 * 临时性的为rank值生成rowkey，以后要废弃
	 * @param roomid
	 * @return
	 */
	public static  String createRowKeyForRank614(Integer roomid)
	{
		if(roomid<0||roomid==null) return  null;
		Integer mod = roomid % 10;
		String modStr = mod + "";
		if(mod < 10) {
			modStr = "0" + mod;
		}
		String rowkey = modStr + "_" + roomid + "_" + "2016-06-16";
		return rowkey;
	}


	public static void main(String[] args) throws Exception {
/*		HbaseUtils util = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
		Long a=  System.currentTimeMillis();
		Tuple3<String,String,Map<String,String>> tuple3 =  util.getOneRecord("02_roomid","popularNumK");
		Map map =  tuple3._3();
		String jsonstr =   map.get("roomuid").toString();
		//System.out.println(jsonstr);
		Map roomuid =   (Map<String,String>)JSON.parse(jsonstr);
		  System.out.println(roomuid.get("912077093"));
		Long b = System.currentTimeMillis();
		System.out.println(b-a);*/

		HbaseUtils util = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
		Long a=  System.currentTimeMillis();
		Integer rank =  util.getRank("06_2131776_2016-07-03","base_info");

		//Integer rank2 = (rank==null?rank:-1);
		System.out.println(rank);
		Long b = System.currentTimeMillis();
		System.out.println(b-a);


		/*HbaseUtils util = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
		Long a=  System.currentTimeMillis();
		Tuple3<String,String,Map<String,String>> tuple3 =  util.getOneRecord("05_144765_2016-06-17","popularNumK");
		Map map =  tuple3._3();
		Iterator it = map.keySet().iterator();
		while (it.hasNext())
		{
			String key = it.next().toString();
			String value = map.get(key).toString();
			System.out.println(CalendarUtil.getDetailDateFormat(Long.parseLong(key))+"---"+value);
		}
		Long b = System.currentTimeMillis();
		System.out.println(b-a);*/

		/*HbaseUtils util = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
		Long a=  System.currentTimeMillis();
		Tuple3<String,String,Map<String,String>> tuple3 =  util.getOneRecord("11_danger","popularNumK");
		Map map =  tuple3._3();
		Iterator it = map.keySet().iterator();
		while (it.hasNext())
		{

			String key = it.next().toString();
			String value = map.get(key).toString();
			System.out.println(key+"---"+value);
		}
		Long b = System.currentTimeMillis();
		System.out.println(b-a);*/

		/*HbaseUtils util = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
		Long a=  System.currentTimeMillis();
		List<String> rowkey = new ArrayList<String>();
		rowkey.add(createRowKeyForRank614(171699));
		rowkey.add(createRowKeyForRank614(174213));
		rowkey.add(createRowKeyForRank614(180103));
		rowkey.add(createRowKeyForRank614(181957));
		rowkey.add(createRowKeyForRank614(185224));
		Map ranks =  util.getRanks(rowkey,"base_info","rank");
		Long b = System.currentTimeMillis();
		System.out.println(b-a);*/
	}

}
