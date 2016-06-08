package com.youku.ddshow.antispam.ugc.bayes.ml;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.CalendarUtil;
import com.youku.ddshow.antispam.utils.HbaseUtils;
import com.youku.ddshow.antispam.utils.Utils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.MqKafaUtil;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by dongjian on 2016/5/31.
 * 全民直播人气预警
 */
public class PeopleLiveScreenStatAlert {
    private static final Pattern SPACE = Pattern.compile("\t");
    private static HbaseUtils test_db = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
    private static  List<Put> putList = new ArrayList<Put>();

    public static void main(String[] args) {

        //************************************开发用**************************************************

/*        if (args.length < 4) {
            System.err.println("Usage: PeopleLiveScreenStatAlert <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("PeopleLiveScreenStatAlert").setExecutorEnv("file.encoding","UTF-8").setMaster("local[8]");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));


        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);*/
    //************************************开发用**************************************************

        //************************************线上用**************************************************
        if (args.length < 4) {
            System.err.println("Usage: PeopleLiveScreenStatAlert <token> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("PeopleLiveScreenStatAlert").setExecutorEnv("file.encoding","UTF-8");
        // Create the context with 60 seconds batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                MqKafaUtil.createStream(jssc, args[1], topicMap, StorageLevel.MEMORY_AND_DISK_SER(), args[0]);
        //************************************线上用**************************************************

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaDStream<ArrayList<String>> words = lines.map(new Function<String, ArrayList<String>>() {
            @Override
            public ArrayList<String> call(String s) {
                return Lists.newArrayList(SPACE.split(s));
            }
        });

        /**
         * 过滤字段个数大于22的，为下面的filter做准备
         */
        JavaDStream<ArrayList<String>> words2 =     words.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.size()>22;
            }
        });

        /**
         * 过滤直播间类型为t_people_live_screen_stat
         */
        JavaDStream<ArrayList<String>> words3 =     words2.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.get(22).equals("t_people_live_screen_stat");
            }
        });

        /**
         * 解析日志中的json
         */
       JavaPairDStream<String, String> mapToPair =   words3.mapToPair(new PairFunction<ArrayList<String>, String, String>() {

           @Override
           public Tuple2<String, String> call(ArrayList<String> strings) throws Exception {
               JSONObject dataInfo =   (JSONObject) JSON.parse(strings.get(23));
               String roomId =   dataInfo.getJSONObject("dataInfo").get("roomId").toString();// 房间id
               String popularNum = dataInfo.getJSONObject("dataInfo").get("popularNum").toString();// 人气值
               String updateTime = dataInfo.getJSONObject("dataInfo").get("updateTime").toString();// 更新时间
               Tuple2<String, String> tuple = new Tuple2<String, String>(roomId,updateTime+"_"+popularNum);
               return tuple;
           }
       });

        /**
         * 根据房间进行reduce聚合
         */
        JavaPairDStream<String, String> reduceWindow =  mapToPair.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s+"-"+s2;
            }
        });

        /**
         * 把reduce聚合后的value转换成map
         */
        JavaPairDStream<String, Map<Long,Integer>> orderedMap =    reduceWindow.mapValues(new Function<String, Map<Long,Integer>>() {
            @Override
            public  Map<Long,Integer> call(String s)
            {
                return Utils.str2Map(s);
            }
        });

        /**
         * 把 时间戳-人气值 转换为  时间戳-k  其中k值为人气值的差值/时间戳的差值
         */
        JavaPairDStream<String, Map<Long,Long>> scoreList =  orderedMap.mapValues(new Function<Map<Long,Integer>, Map<Long,Long>>() {

            @Override
            public Map<Long,Long> call(Map<Long,Integer> map)
            {
                 return Utils.Map2KMap(map);
            }
        });

        /**
         * 把结果转换为存储的格式
         */
        JavaDStream<String> saveList =    scoreList.map(new Function<Tuple2<String,Map<Long,Long>>, String>() {
            @Override
            public  String call(Tuple2<String,Map<Long,Long>> tuple2)
            {
                String roomId =  tuple2._1();
                Map<Long,Long> map =   tuple2._2();

                if(map!=null)
                {
                    Integer roomid =   Integer.parseInt(roomId);
                    Integer mod = roomid % 10;
                    String modStr = mod + "";
                    if(mod < 10) {
                        modStr = "0" + mod;
                    }
                    String statDate = CalendarUtil.getToday();
                    String rowkey = modStr + "_" + roomId + "_" + statDate;
                    System.out.println("rowkey------------------------------->"+rowkey);
                    Put put = new Put(Bytes.toBytes(rowkey));

                    Iterator it =  map.keySet().iterator();
                    while (it.hasNext())
                    {
                        Long timeStamp = (Long)it.next();
                        Long popularK =  map.get(timeStamp);
                      //  System.out.println("popularNumK"+"qulifiter:"+timeStamp+"value:"+popularK);
                        put.add(Bytes.toBytes("popularNumK"), Bytes.toBytes(String.valueOf(timeStamp)), Bytes.toBytes(String.valueOf(popularK)));
                    }
                   // putList.add(put);
                    if(put.size()>0)
                    {
                        List<Put> putList = new ArrayList<Put>();
                        putList.add(put);
                        synchronized (test_db) {
                            test_db.addDataBatch(putList);
                        }
                        putList.clear();
                    }
                }
                return roomId+"_true";
            }
        });


      /*  scoreList.foreachRDD(new Function2<JavaPairRDD<String, Map<Long, Long>>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<String, Map<Long, Long>> stringMapJavaPairRDD, Time time) throws Exception {
                List<Tuple2<String, Map<Long,Long>>> list =   stringMapJavaPairRDD.collect();
                for( Tuple2<String, Map<Long,Long>> tuple2 : list)
                {
                     String roomId =  tuple2._1();
                     Map<Long,Long> map =   tuple2._2();

                    if(map!=null)
                    {
                         Integer roomid =   Integer.parseInt(roomId);
                          Integer mod = roomid % 10;
                          String modStr = mod + "";
                        if(mod < 10) {
                            modStr = "0" + mod;
                        }
                        String statDate = CalendarUtil.getToday();
                        String rowkey = modStr + "_" + roomId + "_" + statDate;
                        Put put = new Put(Bytes.toBytes(rowkey));

                       Iterator it =  map.keySet().iterator();
                        while (it.hasNext())
                        {
                            Long timeStamp = (Long)it.next();
                            Long popularK =  map.get(timeStamp);
                            put.add(Bytes.toBytes("popularNumK"), Bytes.toBytes(timeStamp), Bytes.toBytes(String.valueOf(popularK)));
                        }
                        putList.add(put);
                        synchronized (test_db) {
                            test_db.addDataBatch(putList);
                        }
                        putList.clear();
                    }
                }
                return null;
            }
        });*/

        saveList.print(5000);
        jssc.start();
        jssc.awaitTermination();
    }
}
