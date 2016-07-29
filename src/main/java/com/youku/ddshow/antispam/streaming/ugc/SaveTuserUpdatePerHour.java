package com.youku.ddshow.antispam.streaming.ugc;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.model.TUser;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
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
public class SaveTuserUpdatePerHour {
    private static final Pattern SPACE = Pattern.compile("\t");
    public static void main(final String[] args) throws IOException {
        //************************************开发用**************************************************
       /* if (args.length < 5) {
            System.err.println("Usage: SaveTuserUpdatePerHour <zkQuorum> <group> <topics> <numThreads> <master>");
            System.exit(1);
        }
        SparkConf sparkConf = new SparkConf().setAppName("SaveTuserUpdatePerHour").setExecutorEnv("file.encoding","UTF-8").setMaster("local[8]");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(args[9],"SaveTuserUpdatePerHour", new Duration(2000),System.getenv("SPARK_HOME"),JavaSparkContext.jarOfClass(SaveTuserUpdatePerHour.class));

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
            System.err.println("Usage: SaveTuserUpdatePerHour <token> <group> <topics> <numThreads> <master> <dutationg> <savepath>");
            System.exit(1);
        }
        final  Long dutationg = Long.parseLong(args[5]);
        SparkConf sparkConf = new SparkConf().setAppName("SaveTuserUpdatePerHour").setExecutorEnv("file.encoding","UTF-8");
        final JavaStreamingContext jssc = new JavaStreamingContext(args[4],"SaveTuserUpdatePerHour", new Duration(dutationg),System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(SaveTuserUpdatePerHour.class));
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

        /**
         * {
               "dataMap": {
                   "faceUrl120": "null",
                   "nickName": "null",
                   "mobile": "null",
                   "userId": "822542739",
                   "anchorLevel": "0",
                   "faceUrl": "null",
                   "faceUrlBig": "null",
                   "isShowing": "0",
                   "operate": "99",
                   "userLevel": "0",
                   "isUp": "false",
                   "faceUrl40": "null",
                   "specialEffects": "0",
                   "id": "KAFKA_MSG_USER_e836fa4389eb4dbb9bc59a0ed52c6922",
                   "email": "null"
         },
             "mapNames": {}
         }
         */

        lines.map(new Function<String, TUser>() {
            @Override
            public TUser call(String s) throws Exception {
                JSONObject  dataMap =  LogUtils.getTUserUpdate(s);
                TUser t_user = new TUser();
                t_user.setAnchorLevel(dataMap.containsKey("anchorLevel")?dataMap.getString("anchorLevel"):"");
                t_user.setEmail(dataMap.containsKey("email")?dataMap.getString("email"):"");
                t_user.setFaceUrl(dataMap.containsKey("faceUrl")?dataMap.getString("faceUrl"):"");
                t_user.setFaceUrl40(dataMap.containsKey("faceUrl40")?dataMap.getString("faceUrl40"):"");
                t_user.setFaceUrl120(dataMap.containsKey("faceUrl120")?dataMap.getString("faceUrl120"):"");
                t_user.setFaceUrlBig(dataMap.containsKey("faceUrlBig")?dataMap.getString("faceUrlBig"):"");
                t_user.setId(dataMap.containsKey("id")?dataMap.getString("id"):"");
                t_user.setIsShowing(dataMap.containsKey("isShowing")?dataMap.getString("isShowing"):"");
                t_user.setIsUp(dataMap.containsKey("isUp")?dataMap.getString("isUp"):"");
                t_user.setMobile(dataMap.containsKey("mobile")?dataMap.getString("mobile"):"");
                t_user.setNickName(dataMap.containsKey("nickName")?dataMap.getString("nickName"):"");
                t_user.setOperate(dataMap.containsKey("operate")?dataMap.getString("operate"):"");
                t_user.setSpecialEffects(dataMap.containsKey("specialEffects")?dataMap.getString("specialEffects"):"");
                t_user.setUserId(dataMap.containsKey("userId")?dataMap.getString("userId"):"");
                t_user.setUserLevel(dataMap.containsKey("userLevel")?dataMap.getString("userLevel"):"");
                return t_user;
            }
        }).mapToPair(new PairFunction<TUser, String, String>() {
            @Override
            public Tuple2<String, String> call(TUser tUser) throws Exception {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(tUser.getAnchorLevel()).append(SPACE).append(tUser.getEmail())
                        .append(SPACE).append(tUser.getFaceUrl()).append(SPACE)
                        .append(tUser.getFaceUrl40()).append(SPACE)
                        .append(tUser.getFaceUrl120()).append(SPACE)
                        .append(tUser.getFaceUrlBig()).append(SPACE)
                        .append(tUser.getId()).append(SPACE)
                        .append(tUser.getIsShowing()).append(SPACE)
                        .append(tUser.getIsUp()).append(SPACE)
                        .append(tUser.getMobile()).append(SPACE)
                        .append(tUser.getNickName()).append(SPACE)
                        .append(tUser.getOperate()).append(SPACE)
                        .append(tUser.getSpecialEffects()).append(SPACE)
                        .append(tUser.getUserId()).append(SPACE)
                        .append(tUser.getUserLevel());
                return new Tuple2<String, String>("",stringBuilder.toString());
            }
        }).groupByKey().mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> strings) throws Exception {
                StringBuilder stringBuilder = new StringBuilder();
                for(String str :strings )
                {
                    stringBuilder.append(str).append("\n\r");
                }
                return stringBuilder.toString();
            }
        }).map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> stringStringTuple2) throws Exception {
                return stringStringTuple2._2();
            }
        }).foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                stringJavaRDD.saveAsTextFile(args[6]+time);
                return null;
            }
        });
       /* JavaDStream<ArrayList<String>> splited = lines.map(new Function<String, ArrayList<String>>() {
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
*/
        jssc.start();
        jssc.awaitTermination();
    }
}
