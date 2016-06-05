package com.youku.ddshow.antispam.utils;

import com.youku.ddshow.antispam.model.PropertiesType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wangqiao
 * @date 2016-4-12 下午3:37:35
 */
public class HbaseDeleteFamilyTable {

    private Configuration conf = null;

    private static final Logger LOG = LoggerFactory.getLogger(HbaseCreateTable.class);

    public HbaseDeleteFamilyTable(PropertiesType propertiesType) {
        conf = HBaseConfiguration.create();
        PropertiesUtil properties = new PropertiesUtil(propertiesType.getValue());
        conf.set("hbase.zookeeper.quorum", properties.getValue("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientPort", properties.getValue("hbase.zookeeper.property.clientPort"));
    }


    /**
     * 删除列
     * 
     * @param tableName
     *            表名
     * @param family
     *            列名
     * @throws Exception
     */
    public void deleteFamily(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(tableName));
        admin.disableTable(tableName);
        for (int i = 0; i < family.length; i++) {
            String str = family[i];
            System.out.println(str);
            desc.removeFamily(Bytes.toBytes(str));
        }
        
        //modify target table  struture
        admin.modifyTable(Bytes.toBytes(tableName), desc);
        admin.enableTable(tableName);
        admin.close();
        
    }

    public static void main(String[] args) throws Exception {
        //System.setProperty("hadoop.home.dir", "D:\\develop\\soft\\hadoop-2.5.2");
        HbaseDeleteFamilyTable util = new HbaseDeleteFamilyTable(PropertiesType.DDSHOW_HASE_TEST);
        String[] columns = new String[] { "consume_uv","consume_pv","consume_amount", "consume_coin_rmb" };
        util.deleteFamily("lf_t_result_room_min_stats", columns);

    }

}
