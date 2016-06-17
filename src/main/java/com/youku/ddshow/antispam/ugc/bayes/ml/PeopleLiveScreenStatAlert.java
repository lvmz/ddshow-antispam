package com.youku.ddshow.antispam.ugc.bayes.ml;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.CalendarUtil;
import com.youku.ddshow.antispam.utils.Database;
import com.youku.ddshow.antispam.utils.HbaseUtils;
import com.youku.ddshow.antispam.utils.Utils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.MqKafaUtil;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by dongjian on 2016/5/31.
 * 全民直播人气预警
 */
public class PeopleLiveScreenStatAlert {
    private static final Pattern SPACE = Pattern.compile("\t");
    private static HbaseUtils test_db = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
    private static  List<Put> putList = new ArrayList<Put>();
    private static Map<String, String> concurrentHashMap = new ConcurrentHashMap<String, String>();
    private static Map<String, Integer> roomIdTimes = new ConcurrentHashMap<String, Integer>();

    /**
     * 忽略前1000个值
     * @return
     */
    private  static  boolean isBeginLast(Integer abandon) {

        if (roomIdTimes.containsKey("roomid")) {
            Integer value = roomIdTimes.get("roomid") + 1;
            roomIdTimes.put("roomid", value);

        } else {
            roomIdTimes.put("roomid", 1);
        }

        if(roomIdTimes.get("roomid")>abandon)
        {
            return true;
        }else
        {
            return false;
        }
    }
    /**
     * 初始化房间信息
     * @throws IOException
     */
    private static  void initTroomInfo() throws IOException {
        if(concurrentHashMap.size()==0)
        {
            Tuple3<String,String,Map<String,String>> tuple3 = null;
            synchronized (test_db) {
                tuple3 =    test_db.getOneRecord("00_roomid","popularNumK");
            }
            if(tuple3!=null)
            {
                Map map =  tuple3._3();
                String jsonstr =   map.get("roomuid").toString();
                concurrentHashMap =   (Map<String,String>)JSON.parse(jsonstr);
            }
        }
    }

    /**
     * 清理房间信息
     */
    private  static  void  cleanTroomInfo()
    {
        concurrentHashMap.clear();
    }

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
        if (args.length < 8) {
            System.err.println("Usage: PeopleLiveScreenStatAlert <token> <group> <topics> <numThreads> <prais> <chat> <hbasekey> <abandon>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("PeopleLiveScreenStatAlert").setExecutorEnv("file.encoding","UTF-8");
        // Create the context with 60 seconds batch size

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(10000));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                MqKafaUtil.createStream(jssc, args[1], topicMap, StorageLevel.MEMORY_AND_DISK_SER(), args[0]);
        //************************************线上用**************************************************


        final Integer praisthreshold = Integer.parseInt(args[4]);
        final Integer chatthreshold = Integer.parseInt(args[5]);
        final String hbasekey = args[6];
        final Integer abandon =  Integer.parseInt(args[7]);
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws IOException {
                initTroomInfo();// 初始化房间信息
                return tuple2._2();
            }
        });
       /* *
         * 通过分隔符切割字符*/

        JavaDStream<ArrayList<String>> splited = lines.map(new Function<String, ArrayList<String>>() {
            @Override
            public ArrayList<String> call(String s) {
                return Lists.newArrayList(SPACE.split(s));
            }
        });

      /*  *
         * 过滤字段个数大于22的，为下面的filter做准备*/

        JavaDStream<ArrayList<String>> bigThen22 =     splited.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.size()>6;
            }
        });

//------------------------------点赞 t_user_praise_record-----------------------
        JavaDStream<ArrayList<String>> t_user_praise_record =     bigThen22.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.get(6).equals("t_user_praise_record");
            }
        });

        JavaPairDStream<String, Integer> t_user_praise_record_pair =    t_user_praise_record.mapToPair(new PairFunction<ArrayList<String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(ArrayList<String> strings) throws Exception {

                JSONObject dataInfo =   (JSONObject) JSON.parse(strings.get(7));
                String anchorId =  dataInfo.getJSONObject("dataInfo").get("anchorId").toString();// 播客id
                String count =  dataInfo.getJSONObject("dataInfo").get("count").toString();// 播客id
                Integer countInt = 0;
                try
                {
                    countInt =  Integer.parseInt(count);
                }catch (Exception e)
                {
                    e.printStackTrace();
                }
                return new Tuple2<String, Integer>(anchorId,countInt);
            }
        });


        JavaPairDStream<String, Integer> t_user_praise_record_pair_reduce =    t_user_praise_record_pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        JavaPairDStream<String, Integer> t_user_praise_record_pair_reduce_roomid =    t_user_praise_record_pair_reduce.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(Tuple2<String,Integer> tuple2) throws IOException
            {

                if(concurrentHashMap.size()>0)
                {
                   if(concurrentHashMap.get(tuple2._1())!=null)
                    {
                        return new Tuple2<String, Integer>(concurrentHashMap.get(tuple2._1()),tuple2._2());
                    }else{
                        return new Tuple2<String, Integer>(tuple2._1(),tuple2._2());
                    }
                }else
                {
                    return new Tuple2<String, Integer>(tuple2._1(),tuple2._2());
                }
            }
        });


        //------------------------------评论 t_chat----------------------
        JavaDStream<ArrayList<String>> t_chat =     bigThen22.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.get(6).equals("t_chat");
            }
        });

        JavaPairDStream<String, Integer> t_chat_pair =    t_chat.mapToPair(new PairFunction<ArrayList<String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(ArrayList<String> strings) throws Exception {
                JSONObject dataInfo =   (JSONObject) JSON.parse(strings.get(7));
                String roomId =  dataInfo.getJSONObject("dataInfo").get("roomId").toString();// 播客id 即被留言用户
                return new Tuple2<String, Integer>(roomId,1);
            }
        });

        JavaPairDStream<String, Integer> t_chat_pair_reduce =    t_chat_pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });


        JavaPairDStream<String, scala.Tuple2<Optional<Integer>, Optional<Integer>>>  praiseJoinChat =    t_user_praise_record_pair_reduce_roomid.fullOuterJoin(t_chat_pair_reduce);
        JavaPairDStream<String, Tuple3<Integer, Integer, Integer>> praiseJoinChatOnekey =  praiseJoinChat.mapToPair(new PairFunction<Tuple2<String,Tuple2<Optional<Integer>,Optional<Integer>>>, String, Tuple3<Integer,Integer,Integer>>() {
            @Override
            public  Tuple2<String,Tuple3<Integer,Integer,Integer>> call(Tuple2<String,Tuple2<Optional<Integer>,Optional<Integer>>> tuple2) throws IOException {
                String roomidstr =  tuple2._1();
                Integer praiseNum =   tuple2._2._1().or(0);
                Integer chatNum  = tuple2._2._2().or(0);
                Integer roomid =   Integer.parseInt(roomidstr);
                return new Tuple2<String, Tuple3<Integer,Integer,Integer>>("outputkey",new Tuple3<Integer, Integer, Integer>(roomid,praiseNum,chatNum));
            }
        });

        JavaPairDStream<String, Iterable<Tuple3<Integer, Integer, Integer>>> stringIterableJavaPairDStream = praiseJoinChatOnekey.groupByKey();

        JavaDStream<java.lang.String> result =   stringIterableJavaPairDStream.map(new Function<Tuple2<String,Iterable<Tuple3<Integer, Integer, Integer>>>, String>() {

            @Override
            public String call(Tuple2<String, Iterable<Tuple3<Integer, Integer, Integer>>> stringIterableTuple2) throws Exception {
                List<String> rowkeys = new ArrayList<String>();
                List<Put> putList = new ArrayList<Put>();
                Map ranks = new HashMap();

                // 第一个循环，用来批量获取rank值
                for(Tuple3<Integer, Integer, Integer> tuple3 : stringIterableTuple2._2())
                {
                    Integer roomid = tuple3._1();
                    Integer praiseNum = tuple3._2();
                    Integer chatNum = tuple3._3();
                    rowkeys.add(test_db.createRowKeyForRank614(roomid));
                }
                synchronized (test_db) {
                     ranks =  test_db.getRanks(rowkeys,"base_info","rank"); //批量获取rank值
                }

                //第二个循环，用来批量插入
                for(Tuple3<Integer, Integer, Integer> tuple3 : stringIterableTuple2._2())
                {
                    Integer roomid = tuple3._1();
                    Integer praiseNum = tuple3._2();
                    Integer chatNum = tuple3._3();
                    Object rankstr =   ranks.get(roomid.toString());
                    String rowkey = test_db.createRowKey(roomid);
                    if(rankstr ==null)
                    {
                        rankstr = "-1";
                    }

                    Integer rank = Integer.parseInt(rankstr.toString());

                    if((praiseNum>praisthreshold||chatNum>chatthreshold)&&roomid>100000) // 如果点赞数量或评论数量超过阈值且不是秀场的主播
                    {
                        if(rank<=0&&isBeginLast(abandon)) //如果是新主播或有前科的主播且是第三次获取这个值
                        {
                            String rowkeyResult = hbasekey;
                            Put put = new Put(Bytes.toBytes(rowkeyResult));
                            put.add(Bytes.toBytes("popularNumK"), Bytes.toBytes(String.valueOf(roomid)), Bytes.toBytes(String.valueOf(praiseNum+"_"+chatNum+"_"+rank)));
                            List<Put> putListResult = new ArrayList<Put>();
                            putListResult.add(put);
                            synchronized (test_db) {
                                test_db.addDataBatch(putListResult);
                            }
                            putListResult.clear();
                        }
                    }

                    if(rowkey!=null)
                    {
                        Put put = new Put(Bytes.toBytes(rowkey));
                        String popularK = praiseNum+"_"+chatNum+"_"+rank.toString();
                        put.add(Bytes.toBytes("popularNumK"), Bytes.toBytes(String.valueOf(System.currentTimeMillis())), Bytes.toBytes(String.valueOf(popularK)));
                        putList.add(put);
                    }
                }
                synchronized (test_db) {
                    test_db.addDataBatch(putList);//批量写入rank值
                }
                putList.clear();
                return stringIterableTuple2._1()+"true";
            }
        });
        result.print(5000);

        jssc.start();
        jssc.awaitTermination();
    }
}
