package com.youku.ddshow.antispam.streaming.ugc;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.UgcCommentLog;
import com.youku.ddshow.antispam.utils.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
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
public class UgcCommentAntiSpam {
    private static final Pattern SPACE = Pattern.compile("\t");
    public static void main(String[] args) {
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
            System.err.println("Usage: UgcCommentAntiSpam <token> <group> <topics> <numThreads> <master> <dutationg> <window> <split>");
            System.exit(1);
        }
        final  Long dutationg = Long.parseLong(args[5]);
        final  Long window = Long.parseLong(args[6]);
        final  Long split = Long.parseLong(args[7]);
        SparkConf sparkConf = new SparkConf().setAppName("UgcCommentAntiSpam").setExecutorEnv("file.encoding","UTF-8");
        // Create the context with 60 seconds batch size

        final JavaStreamingContext jssc = new JavaStreamingContext(args[4],"UgcCommentAntiSpam", new Duration(dutationg),System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(UgcCommentAntiSpam.class));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }
        JavaPairReceiverInputDStream<String, String> messages =
                MqKafaUtil.createStream(jssc, args[1], topicMap, StorageLevel.MEMORY_AND_DISK_SER(), args[0]);
        //************************************线上用**************************************************



          /* *
         * 过滤字段个数大于22的，为下面的filter做准备*/
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

        JavaDStream<ArrayList<String>> t_ugc_comment =     bigThen6.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                return strings.get(6).equals("t_ugc_comment");
            }
        });
        JavaDStream<java.util.ArrayList<java.lang.String>> t_ugc_comment_level0_role129 =     t_ugc_comment.filter(new Function<ArrayList<String>, Boolean>() {
            @Override
            public Boolean call(ArrayList<String> strings) throws Exception {
                if(StringUtils.isNotBlank(strings.get(7)))
                {
                    JSONObject dataJson = LogUtils.getUgcDataJson(strings.get(7));
                    if (dataJson.containsKey("userLevel"))
                    {
                       if("0".equals(dataJson.getString("userLevel"))&&"129".equals(dataJson.getString("role")))
                       {
                           return true;
                       }else
                       {
                           return false;
                       }
                    }else
                    {
                        return false;
                    }
                }else
                {
                    return false;
                }
            }
        });

        JavaDStream<UgcCommentLog> t_ugc_comment_level0_role129_Object =   t_ugc_comment_level0_role129.map(new Function<ArrayList<String>, UgcCommentLog>() {
            @Override
            public UgcCommentLog call(ArrayList<String> strings) throws Exception {
                UgcCommentLog ugcCommentLog = new UgcCommentLog();

                ugcCommentLog.setIp(LogUtils.getIpBySeuenceId(strings.get(5)));
                ugcCommentLog.setToken(LogUtils.getTokenBySeuenceId(strings.get(5)));
                   if(StringUtils.isNotBlank(strings.get(7)))
                   {
                       JSONObject dataJson = LogUtils.getUgcDataJson(strings.get(7));
                       ugcCommentLog.setCommenterId(dataJson.containsKey("commenter")?dataJson.getInteger("commenter"):0);
                       ugcCommentLog.setCommentId(dataJson.containsKey("id")?dataJson.getInteger("id"):0);
                       ugcCommentLog.setEntityId(dataJson.containsKey("entityId")?dataJson.getInteger("entityId"):0);
                       ugcCommentLog.setContent(dataJson.containsKey("content")?dataJson.getString("content"):"");
                       ugcCommentLog.setNickName(dataJson.containsKey("nickName")?dataJson.getString("nickName"):"");
                       ugcCommentLog.setTimestamp(dataJson.containsKey("timestamp")?dataJson.getLongValue("timestamp"):0L);
                   }

                return ugcCommentLog;
            }
        });
        JavaPairDStream<Integer, Integer> CommenterIdPair =  t_ugc_comment_level0_role129_Object.mapToPair(new PairFunction<UgcCommentLog, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(UgcCommentLog ugcCommentLog) throws Exception {
                return new Tuple2<Integer, Integer>(ugcCommentLog.getCommenterId(),1);
            }
        });

        JavaPairDStream<String, Integer> ContentPair =  t_ugc_comment_level0_role129_Object.mapToPair(new PairFunction<UgcCommentLog, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(UgcCommentLog ugcCommentLog) throws Exception {
                return new Tuple2<String, Integer>(ugcCommentLog.getContent(),1);
            }
        });

        CommenterIdPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).print(5000);

        ContentPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        }).print(5000);
      /*  CommenterIdPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        },new Duration(window),new Duration(split)).print(5000);

        ContentPair.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        },new Duration(window),new Duration(split)).print(5000);*/

        t_ugc_comment_level0_role129_Object.print(5000);
        jssc.start();
        jssc.awaitTermination();
    }
}
