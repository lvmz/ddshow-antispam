package com.youku.ddshow.antispam.streaming.ugc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.IntegerCodec;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.*;
import com.youku.ddshow.antispam.model.UgcChat;
import com.youku.ddshow.antispam.utils.CalendarUtil;
import com.youku.ddshow.antispam.utils.ContentKeyWordFilter;
import com.youku.ddshow.antispam.utils.Database;
import com.youku.ddshow.antispam.utils.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.MqKafaUtil;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by dongjian on 2016/6/21.
 */
public class UgcCommentAntiSpamByChatContent {
    private static final Pattern SPACE = Pattern.compile("\t");
    private static Database _db = null;
    public static void main(String[] args) throws IOException {
        //************************************开发用**************************************************
       /* if (args.length < 5) {
            System.err.println("Usage: UgcCommentAntiSpam <zkQuorum> <group> <topics> <numThreads> <master>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("UgcCommentAntiSpam").setExecutorEnv("file.encoding","UTF-8").setMaster("local[8]");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(args[9],"UgcCommentAntiSpam", new Duration(2000),System.getenv("SPARK_HOME"),JavaSparkContext.jarOfClass(UgcCommentAntiSpam.class));

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
        if (args.length < 5) {
            System.err.println("Usage: UgcCommentAntiSpam <token> <group> <topics> <numThreads> <master> <dutationg> <connections> <rediskey>");
            System.exit(1);
        }


        final  Long dutationg = Long.parseLong(args[5]);
        final  Integer connections = Integer.parseInt(args[6]);
        final  String rediskey = args[7];
        SparkConf sparkConf = new SparkConf().setAppName("UgcCommentAntiSpam").setExecutorEnv("file.encoding","UTF-8");
        // Create the context with 60 seconds batch size

        final JavaStreamingContext jssc = new JavaStreamingContext(args[4],"UgcCommentAntiSpam", new Duration(dutationg),System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(UgcCommentAntiSpamByChatContent.class));
        _db  =  new Database(PropertiesType.DDSHOW_STAT_ONLINE);
        ContentKeyWordFilter contentKeyWordFilter =  new ContentKeyWordFilter(PropertiesType.DDSHOW_QKD_TEST);

        final Broadcast<Database> broadcast   =   jssc.sc().broadcast(_db);
        final Broadcast<ContentKeyWordFilter> contentKeyWordFilterBroadcast   =   jssc.sc().broadcast(contentKeyWordFilter);
        final Accumulator<Integer> intAccumulator =  jssc.sc().intAccumulator(0);
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
        JavaDStream<ArrayList<String>> bigThen6 =     splited.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.size()>6;
            }
        });

        /*JavaDStream<ArrayList<String>> t_room_user_count_detail  =     bigThen6.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.get(6).equals("t_room_user_count_detail");
            }
        });

      JavaDStream<UserRoomOnlineStatInfo> t_room_user_count_detail_Object =   t_room_user_count_detail.map(new Function<ArrayList<String>, UserRoomOnlineStatInfo>() {
            @Override
            public UserRoomOnlineStatInfo call(ArrayList<String> strings) throws Exception {

                UserRoomOnlineStatInfo log = new UserRoomOnlineStatInfo();


                log.setVersion(LogUtils.getIntValue(strings.get(0)));
                log.setTimestamp(LogUtils.getLongValue(strings.get(1)));
                log.setIp(LogUtils.getStringValue(strings.get(2)));
                log.setAppId(LogUtils.getIntValue(strings.get(3)));
                log.setCategory(LogUtils.getStringValue(strings.get(4)));
                log.setCpsMid(LogUtils.getIntValue(strings.get(5)));
                log.setCpsCid(LogUtils.getIntValue(strings.get(6)));
                log.setCpsPid(LogUtils.getIntValue(strings.get(7)));
                log.setCpsLpid(LogUtils.getIntValue(strings.get(8)));
                log.setMachineId(LogUtils.getStringValue(strings.get(9)));
                log.setYtId(LogUtils.getIntValue(strings.get(10)));
                log.setUserId(LogUtils.getIntValue(strings.get(11)));
                log.setTId(LogUtils.getIntValue(strings.get(12)));
                log.setUserType(LogUtils.getIntValue(strings.get(13)));
                log.setVip(LogUtils.getIntValue(strings.get(14))==1?true:false);
                log.setRoom(LogUtils.getIntValue(strings.get(15)));
                log.setAnchorId(LogUtils.getIntValue(strings.get(16)));
                log.setAppVersion(LogUtils.getStringValue(strings.get(17)));
                log.setOsVersion(LogUtils.getStringValue(strings.get(18)));
                log.setDeviceToken(LogUtils.getStringValue(strings.get(19)));
                log.setFk_origin_user(LogUtils.getIntValue(strings.get(20)));
                log.setFk_target_user(LogUtils.getIntValue(strings.get(21)));
                log.setUgcType(LogUtils.getStringValue(strings.get(22)));
                log.setDataInfo(LogUtils.getStringValue(strings.get(23)));
                log.setSequenceId(LogUtils.getIntValue(strings.get(24)));
                return log;
            }
        });

        t_room_user_count_detail_Object.mapToPair(new PairFunction<UserRoomOnlineStatInfo, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(UserRoomOnlineStatInfo userRoomOnlineStatInfo) throws Exception {
                return new Tuple2<String, Integer>(userRoomOnlineStatInfo.getIp()+"-"+userRoomOnlineStatInfo.getRoom(),1);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                String ip_room = stringIterableTuple2._1();
                String ip = ip_room.split("-")[0]==null?"":ip_room.split("-")[0];
                return new Tuple2<String, Integer>(ip,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._2()>10;
            }
        }).print(5000);*/



        JavaDStream<ArrayList<String>> t_chat =     bigThen6.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.get(6).equals("t_chat");
            }
        });


        /**
         *  {
                "dataInfo": {
                    "anchorLevel": 0,
                    "content": "[亲亲]",
                    "createDate": {
                        "date": 12,
                        "day": 2,
                        "hours": 16,
                        "minutes": 15,
                        "month": 6,
                        "seconds": 29,
                        "time": 1468311329305,
                        "timezoneOffset": -480,
                        "year": 116
                    },
                    "id": 0,
                    "methodName": "",
                    "originUserId": 944483774,
                    "originUserName": "矮油~我去",
                    "roomId": 154680,
                    "screenId": 2547180,
                    "single": 0,
                    "status": 0,
                    "targetUserId": 900198977,
                    "targetUserName": "AY_baby小雨",
                    "userLevel": 0
         }
         }
         */
        JavaDStream<UgcChat> t_chat_Object =   t_chat.map(new Function<ArrayList<String>, UgcChat>() {
            @Override
            public UgcChat call(ArrayList<String> strings) throws Exception {

                UgcChat ugcChat = new UgcChat();
                ugcChat.setIp(LogUtils.getIpBySeuenceId(strings.get(5)));
                ugcChat.setToken(LogUtils.getTokenBySeuenceId(strings.get(5)));
                if(StringUtils.isNotBlank(strings.get(7)))
                {
                    JSONObject dataJson = LogUtils.getUgcDataJson(strings.get(7));
                    ugcChat.setAnchorLevel(dataJson.containsKey("anchorLevel")?dataJson.getInteger("anchorLevel"):0);
                    ugcChat.setContent(dataJson.containsKey("content")?dataJson.getString("content"):"");
                    JSONObject createDateJson =  JSONObject.parseObject(dataJson.containsKey("createDate")?dataJson.getString("createDate"):"");
                    ugcChat.setCreateDate(createDateJson.containsKey("time")?createDateJson.getString("time"):"");
                    ugcChat.setId(dataJson.containsKey("id")?dataJson.getInteger("id"):0);
                    ugcChat.setMethodName(dataJson.containsKey("methodName")?dataJson.getString("methodName"):"");
                    ugcChat.setOriginUserId(dataJson.containsKey("originUserId")?dataJson.getInteger("originUserId"):0);
                    ugcChat.setOriginUserName(dataJson.containsKey("originUserName")?dataJson.getString("originUserName"):"");
                    ugcChat.setRoomId(dataJson.containsKey("roomId")?dataJson.getInteger("roomId"):0);
                    ugcChat.setScreenId(dataJson.containsKey("screenId")?dataJson.getInteger("screenId"):0);
                    ugcChat.setSingle(dataJson.containsKey("single")?dataJson.getInteger("single"):0);
                    ugcChat.setStatus(dataJson.containsKey("status")?dataJson.getInteger("status"):0);
                    ugcChat.setTargetUserId(dataJson.containsKey("targetUserId")?dataJson.getInteger("targetUserId"):0);
                    ugcChat.setTargetUserName(dataJson.containsKey("targetUserName")?dataJson.getString("targetUserName"):"");
                    ugcChat.setUserLevel(dataJson.containsKey("userLevel")?dataJson.getInteger("userLevel"):0);
                }
                return ugcChat;
            }
        });

        JavaPairDStream<String, UgcChat> t_chat_Object_Ip =    t_chat_Object.mapToPair(new PairFunction<UgcChat, String, UgcChat>() {
            @Override
            public Tuple2<String, UgcChat> call(UgcChat ugcChat) throws Exception {
                return new Tuple2<String, UgcChat>(ugcChat.getIp(),ugcChat);
            }
        });


        t_chat_Object.mapToPair(new PairFunction<UgcChat, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(UgcChat ugcChat) throws Exception {
                return new Tuple2<String, Integer>(ugcChat.getIp()+"-"+ugcChat.getRoomId(),1);
            }
        }).groupByKey().mapToPair(new PairFunction<Tuple2<String,Iterable<Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                String ip_room = stringIterableTuple2._1();
                String ip = ip_room.split("-")[0]==null?"":ip_room.split("-")[0];
                return new Tuple2<String, Integer>(ip,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2._2()>connections;
            }
        }).leftOuterJoin(t_chat_Object_Ip).map(new Function<Tuple2<String,Tuple2<Integer,Optional<UgcChat>>>, String>() {
             @Override
             public String call(Tuple2<String, Tuple2<Integer, Optional<UgcChat>>> stringTuple2Tuple2) throws Exception {
                 Optional<UgcChat>  optional =  stringTuple2Tuple2._2()._2();
                 UgcChat ugcChat = optional.orNull();
                 if(ugcChat!=null)
                 {
                     ContentKeyWordFilter  contentKeyWordFilter =  contentKeyWordFilterBroadcast.getValue();
                     contentKeyWordFilter.saveSpam2Qkd(ugcChat.getContent(),rediskey,stringTuple2Tuple2._2()._1());
                     StringBuilder stringBuilder = new StringBuilder();
                     stringBuilder.append(stringTuple2Tuple2._2()._1()+"\t")
                     .append(CalendarUtil.getDetailDateFormat(StringUtils.isNotEmpty(ugcChat.getCreateDate())?Long.parseLong(ugcChat.getCreateDate()):0L)+"\t")
                     .append(ugcChat.getIp()+"\t")
                     .append(ugcChat.getRoomId()+"\t")
                     .append(ugcChat.getUserLevel()+"\t")
                     .append(ugcChat.getOriginUserId()+"\t")
                     .append(ugcChat.getTargetUserId()+"\t")
                     .append(ugcChat.getContent()+"\t");
                     return stringBuilder.toString();
                 }else
                 {
                     return null;
                 }
             }
         }).print(5000);



       /* t_chat_Object.filter(new Function<UgcChat, Boolean>() {
            @Override
            public Boolean call(UgcChat UgcChat) throws Exception {
                ContentKeyWordFilter  contentKeyWordFilter =  contentKeyWordFilterBroadcast.getValue();
                return contentKeyWordFilter.isSpamNickName(UgcChat.getContent());
            }
        }).map(new Function<UgcChat, String>() {
            @Override
            public String call(UgcChat UgcChat) throws Exception {
                return UgcChat.getContent();
            }
        }).print(5000);

        t_chat_Object.filter(new Function<UgcChat, Boolean>() {
            @Override
            public Boolean call(UgcChat UgcChat) throws Exception {
                ContentKeyWordFilter  contentKeyWordFilter =  contentKeyWordFilterBroadcast.getValue();
                return contentKeyWordFilter.isSpamNickName(UgcChat.getContent());
            }
        }).foreach(new Function2<JavaRDD<UgcChat>, Time, Void>() {
            @Override
            public Void call(JavaRDD<UgcChat> UgcChatJavaRDD, Time time) throws Exception {

                UgcChatJavaRDD.foreach(new VoidFunction<UgcChat>() {
                    @Override
                    public void call(UgcChat ugcChat) throws Exception {
                        ContentKeyWordFilter  contentKeyWordFilter =  contentKeyWordFilterBroadcast.getValue();
                        System.out.println("spamcontenwithkeyword-------->"+ugcChat.getContent());
                        intAccumulator.add(1);
                        int num =  intAccumulator.value();
                        System.out.println("intAccumulator value-------->"+num);
                       // contentKeyWordFilter.saveSpam2Qkd(JSON.toJSONString(ugcChat),num%1000);
                    }
                });
                return null;
            }
        });*/
        jssc.start();
        jssc.awaitTermination();
    }
}
