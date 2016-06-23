package com.youku.ddshow.antispam.streaming.ugc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.HbaseUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
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
    private static Map<Integer, Integer> roomidUidMap = new ConcurrentHashMap<Integer, Integer>();
    private static Broadcast<JavaPairRDD<String, Iterable<Tuple2<Integer,Integer>>>>   broadcastRoomiduid(JavaStreamingContext jssc,String roomiduidPath)
    {
        JavaSparkContext ctx = jssc.sparkContext();
        JavaRDD<String> textFile = ctx.textFile(roomiduidPath, 1);
        JavaRDD<java.util.List<java.lang.String>> t_room =  textFile.map(new Function<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                System.out.println(s);
                return Arrays.asList(SPACE.split(s));
            }
        });
        JavaPairRDD<String, Tuple2<Integer, Integer>> roomiduidPair =  t_room.mapToPair(new PairFunction<List<String>,String,Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<String, Tuple2<Integer,Integer>> call(List<String> strings) throws Exception {
                Integer roomid =   Integer.parseInt(strings.get(0));
                Integer uid = Integer.parseInt(strings.get(1));
                return new  Tuple2<String, Tuple2<Integer,Integer>>("roomiduid",new Tuple2<Integer, Integer>(roomid,uid));
            }
        });
        JavaPairRDD<String, Iterable<Tuple2<Integer, Integer>>> roomiduidRDD =   roomiduidPair.groupByKey();
       return   jssc.sc().broadcast(roomiduidRDD);
    }
    public static void main(String[] args) {


        //************************************开发用**************************************************
       /* if (args.length < 4) {
            System.err.println("Usage: PeopleLiveScreenStatAlert <zkQuorum> <group> <topics> <numThreads> <prais> <chat> <hbasekey> <abandon> <roomiduid>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("PeopleLiveScreenStatAlert").setExecutorEnv("file.encoding","UTF-8").setMaster("local[8]");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(args[9],"PeopleLiveScreenStatAlert", new Duration(2000),System.getenv("SPARK_HOME"),JavaSparkContext.jarOfClass(PeopleLiveScreenStatAlert.class));

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
            System.err.println("Usage: PeopleLiveScreenStatAlert <token> <group> <topics> <numThreads> <prais> <chat> <hbasekey> <abandon> <roomiduid>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("PeopleLiveScreenStatAlert").setExecutorEnv("file.encoding","UTF-8");
        // Create the context with 60 seconds batch size

       final JavaStreamingContext jssc = new JavaStreamingContext(args[9],"PeopleLiveScreenStatAlert", new Duration(2000),System.getenv("SPARK_HOME"),JavaSparkContext.jarOfClass(PeopleLiveScreenStatAlert.class));



        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        JavaPairReceiverInputDStream<String, String> messages =
                MqKafaUtil.createStream(jssc, args[1], topicMap, StorageLevel.MEMORY_AND_DISK_SER(), args[0]);
        //************************************线上用**************************************************
        final Accumulator<Integer> intAccumulator  = jssc.sc().intAccumulator(0);

        final Integer praisthreshold = Integer.parseInt(args[4]);
        final Integer chatthreshold = Integer.parseInt(args[5]);
        final String hbasekey = args[6];
        final Integer abandon =  Integer.parseInt(args[7]);
        final  String roomiduidPath = args[8];
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws IOException {
                return tuple2._2();
            }
        });


        JavaDStream<ArrayList<String>> splited = lines.map(new Function<String, ArrayList<String>>() {
            @Override
            public ArrayList<String> call(String s) {
                return Lists.newArrayList(SPACE.split(s));
            }
        });

       /* *
         * 过滤字段个数大于22的，为下面的filter做准备*/

        JavaDStream<ArrayList<String>> bigThen22 =     splited.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.size()>6;
            }
        });

        JavaDStream<ArrayList<String>> nothing =     splited.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.size()>1000;
            }
        });
        JavaDStream<java.util.List<java.util.List<java.lang.String>>> sdf =   nothing.map(new Function<ArrayList<String>,  List<List<java.lang.String>> >() {
            @Override
            public  List<List<java.lang.String>>  call(ArrayList<String> strings) throws Exception {

                SparkContext ctx = jssc.sc().sc();
                RDD<String> textFile = ctx.textFile(roomiduidPath, 1);
                JavaRDD<java.util.List<java.lang.String>> t_room =  textFile.toJavaRDD().map(new Function<String, List<String>>() {
                    @Override
                    public List<String> call(String s) throws Exception {
                        System.out.println(s);
                        return Arrays.asList(SPACE.split(s));
                    }
                });
                return t_room.collect();
            }
        });
        sdf.print();
/*//------------------------------点赞 t_user_praise_record-----------------------
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
        });*/

       /* JavaPairDStream<String, Integer> t_user_praise_record_pair_reduce_roomid =    t_user_praise_record_pair_reduce.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
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
        });*/


     /*   //------------------------------评论 t_chat----------------------
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
        });*/


       /* JavaPairDStream<String, Tuple2<Optional<Integer>, Optional<Integer>>>  praiseJoinChat =    t_user_praise_record_pair_reduce_roomid.fullOuterJoin(t_chat_pair_reduce);
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

        JavaDStream<String> result =   stringIterableJavaPairDStream.map(new Function<Tuple2<String,Iterable<Tuple3<Integer, Integer, Integer>>>, String>() {

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
        });*/
       // result.print(5000);

        jssc.start();
        jssc.awaitTermination();
    }
}
