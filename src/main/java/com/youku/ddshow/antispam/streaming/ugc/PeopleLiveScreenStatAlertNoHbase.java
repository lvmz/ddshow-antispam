package com.youku.ddshow.antispam.streaming.ugc;

import com.alibaba.fastjson.JSON;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.HbaseUtils;
import org.apache.hadoop.hbase.client.Put;
import scala.Tuple3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by dongjian on 2016/5/31.
 * 全民直播人气预警
 */
public class PeopleLiveScreenStatAlertNoHbase {
    private static final Pattern SPACE = Pattern.compile("\t");
    private static HbaseUtils test_db = new HbaseUtils(PropertiesType.DDSHOW_HASE_TEST, "lf_t_view_hbase_room_stat");
    private static  List<Put> putList = new ArrayList<Put>();
    private static Map<String, String> concurrentHashMap = new ConcurrentHashMap<String, String>();
    private static Map<String, String> roomidUid = new ConcurrentHashMap<String, String>();
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

      /*  if (args.length < 4) {
            System.err.println("Usage: PeopleLiveScreenStatAlert <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("PeopleLiveScreenStatAlert").setExecutorEnv("file.encoding","UTF-8").setMaster("local[8]");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));


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
 /*       if (args.length < 8) {
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
                MqKafaUtil.createStream(jssc, args[1], topicMap, StorageLevel.MEMORY_AND_DISK_SER(), args[0]);*/
        //************************************线上用**************************************************

       /* JavaPairDStream<java.lang.String, java.lang.String>  line =  messages.mapToPair(new PairFunction<Tuple2<String,String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return new Tuple2<String, String>("kafka",stringStringTuple2._2());
            }
        });
        line.foreachRDD(new Function2<JavaPairRDD<String, String>, Time, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> stringStringJavaPairRDD, Time time) throws Exception {
              final SparkContext sc =  stringStringJavaPairRDD.rdd().sparkContext();
                RDD<String> textFile = stringStringJavaPairRDD
                        .rdd().sparkContext().textFile("D:\\popular", 1);
                JavaRDD<java.util.List<java.lang.String>> t_room =  textFile.toJavaRDD().map(new Function<String, List<String>>() {
                      @Override
                      public List<String> call(String s) throws Exception {
                          //System.out.println(s);
                          return Arrays.asList(SPACE.split(s));
                      }
                  });
                JavaPairRDD<String, scala.Tuple2<Integer, Integer>>  roomiduid =  t_room.mapToPair(new PairFunction<List<String>,String,Tuple2<Integer,Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Integer,Integer>> call(List<String> strings) throws Exception {
                           Integer roomid =   Integer.parseInt(strings.get(0));
                           Integer uid = Integer.parseInt(strings.get(1));
                        return new  Tuple2<String, Tuple2<Integer,Integer>>("roomiduid",new Tuple2<Integer, Integer>(roomid,uid));
                    }
                });
                 roomiduid.groupByKey().mapValues(new Function<Iterable<Tuple2<Integer,Integer>>, Object>() {

                     @Override
                     public Object call(Iterable<Tuple2<Integer, Integer>> tuple2s) throws Exception {

                         for(Tuple2<Integer, Integer> tuple2 : tuple2s)
                         {
                             System.out.println();
                         }

                         return null;
                     }
                 });


                roomiduid.collect();  //用来触发rdd
                return null;
            }
        });

        line.print(5000);
        jssc.start();
        jssc.awaitTermination();*/
    }
}
