package com.youku.ddshow.antispam.ugc.bayes.ml;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.*;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by dongjian on 2016/4/22.
 *
 * 贝叶斯分类器监听kafka实时过滤垃圾评论
 * 反垃圾策略主要分为两部分
 * 1、是一个贝叶斯文本分类器进行判断，需要定时更新模型
 * 2、是一个频次策略，如果1个小时内某id给超过三个以上主播留言超过十条，被视作在发垃圾评论
 */
public class BayesKafkaWorkStreamV2 {
    public  static NaiveBayesModel loadedModel = null;
    private static final Pattern SPACE = Pattern.compile("\t");
    private static final  int timeInteval = 1800;
    private static final  int contentPv = 10;
    private static final  int entityPv = 5;
    private static final  long newCommenterId = 910000000;
    private static Map<String, Integer> concurrentHashMap = new ConcurrentHashMap<String, Integer>();
    private static  Database _db = null;
    private BayesKafkaWorkStreamV2() {
    }

    /**
     * 给同一个动态评论多条，视为狂热粉丝，不是垃圾评论
     * @param commenterId
     * @param entityId
     * @return
     */
    private  static  boolean isNotSpamerId(String commenterId,String entityId)
    {
        String key = commenterId+"-"+entityId;
        if( concurrentHashMap.containsKey(key))
        {
            Integer value = concurrentHashMap.get(key) +1;
            concurrentHashMap.put(key,value);

        }else
        {
            concurrentHashMap.put(key,1);
        }

        if(concurrentHashMap.get(key)>entityPv)
        {
            System.out.println("这是个疑似狂热粉丝 ————》"+commenterId);
            return true;
        }else
        {
            return false;
        }
    }
    private  static  boolean isComment2Many(String commenterId)
    {
        Integer i =0;
        Iterator it =  concurrentHashMap.keySet().iterator();
        while (it.hasNext())
        {
            String key = it.next().toString();
            if(key.indexOf(commenterId)!=-1)
            {
                i++;
            }
        }
        if(i>4)
        {
            return true;
        }else
        {
            return false;
        }
    }
    /**
     * 给定时间评论超过规定数量，视为疑似评论者
     * @param commenterId
     * @return
     */
    private  static  boolean isSpamerId(String commenterId)
    {

        if( concurrentHashMap.containsKey(commenterId))
        {
            Integer value = concurrentHashMap.get(commenterId) +1;
            concurrentHashMap.put(commenterId,value);

        }else
        {
            concurrentHashMap.put(commenterId,1);
        }

        if(concurrentHashMap.get(commenterId)>contentPv)
        {
            System.out.println("这是个疑似垃圾评论者 ————》"+commenterId);
            return true;
        }else
        {
            return false;
        }
    }

    public  static boolean isNewUser(String commenterId)
    {
        Long id = 0L;
        try{
            id =  Long.parseLong(commenterId);
            if(id>newCommenterId)
            {
                return  true;
            }
          }catch ( Exception e)
          {
              e.printStackTrace();
          }
        return  false;
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: BayesKafkaWorkStreamV2 <zkQuorum> <group> <topics> <numThreads> <modelpath>");
            System.err.println("For Example: BayesKafkaWorkStreamV2 10.100.47.142:2181,10.100.47.105:2181,10.100.47.103:2181 test laifengtest_ugc 8 D:\\文本分类\\ugc\\ugcAntispamModel5 ");
            System.exit(1);
        }
        _db  =  new Database(PropertiesType.DDSHOW_STAT_TEST);
        SparkConf sparkConf = new SparkConf().setAppName("BayesKafkaWorkStreamV2").setExecutorEnv("file.encoding","UTF-8").setMaster("local[8]");
        // Create the context with a 1 second batch size
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
        final Accumulator<Integer> At =  jssc.sc().accumulator(0); //累加器

        final SQLContext sqlContext = new SQLContext(jssc.sc());
       String modelPath = args[4];
       final NaiveBayesModel loadedModel =     NaiveBayesModel.load(jssc.sc().sc(),modelPath);

       int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String x) {
                return Lists.newArrayList(SPACE.split(x));
            }
        });
        words.print();


        /**
         * 将JavaDStream转换为普通rdd进行处理
         */
       words.foreach(new Function2<JavaRDD<String>, Time, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {
                At.add(1);
                if((At.value()%timeInteval)==0)
                {

                    System.out.println( concurrentHashMap.size()+"累加器值"+At.value());
                    Iterator it = concurrentHashMap.keySet().iterator();
                    while (it.hasNext())
                    {
                        String key = it.next().toString();
                        System.out.println( key+"-->"+concurrentHashMap.get(key));
                    }
                    concurrentHashMap.clear();
                }
                List<String> listFromStream =   stringJavaRDD.collect();

                JavaRDD<String> resultRDD = null;
                if(listFromStream.size()>1)
                {
                    String[] ipAndToken =   listFromStream.get(listFromStream.size()-3).split("_");
                    String ip =ipAndToken[1];
                    String token = ipAndToken[3];
                    try{
                         JSONObject  dataInfo =   (JSONObject)JSON.parse(listFromStream.get(listFromStream.size()-1));
                         String content =   dataInfo.getJSONObject("dataInfo").get("content").toString();
                         String user_name = dataInfo.getJSONObject("dataInfo").get("nickName").toString();
                         String commentId = dataInfo.getJSONObject("dataInfo").get("id").toString();
                         String userLevel = dataInfo.getJSONObject("dataInfo").get("userLevel").toString();
                         String commenterId = dataInfo.getJSONObject("dataInfo").get("commenter").toString();
                         String entityId =   dataInfo.getJSONObject("dataInfo").get("entityId").toString();
                         String timestamp =   dataInfo.getJSONObject("dataInfo").get("timestamp").toString();


                         System.out.println("这是要检测的内容：---》"+content+" "+user_name+" "+commentId+" "+userLevel+" "+commenterId+" "+ CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp))+" "+ip+" "+token);
                         List<String>   listForModel = new ArrayList<String>();
                         listForModel.add(LaifengWordAnalyzer.wordAnalyzer(content));
                         System.out.println("这是分词结果：---》"+LaifengWordAnalyzer.wordAnalyzer(content));
                         JavaSparkContext ctx = new JavaSparkContext(stringJavaRDD.context());
                         DataFrame rescaledData =   BysJava2.String2DataFrame(listForModel, ctx,sqlContext);

                        if(rescaledData!=null)
                        {
                            String labelResult = "";
                            List<String> resultList = new ArrayList<String>();
                            for (Row r : rescaledData.select("features", "label").take(1)) {

                                Vector features = r.getAs(0);
                                Integer label =  r.getInt(1);

                                Double result =  loadedModel.predict(features);

                                if(NickNameFilter.isSpamNickName(user_name)&&isNewUser(commenterId))
                                {

                                    System.out.println("疑似垃圾评论昵称");
                                    synchronized(_db){
                                        _db.execute(String.format("insert into t_result_ugc_comment_antispam_highpv (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
                                                ,commenterId, ip, token,  user_name+"_spam", commentId,
                                                content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));

                                        if(userLevel.equals("0"))
                                        {
                                            _db.execute(String.format("insert into t_result_ugc_comment_antispam (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
                                                    ,commenterId, ip, token,  user_name, commentId,
                                                    content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));
                                        }
                                    }
                                }


                                if(result.shortValue()==0)
                                {
                                    System.out.println("这是条广告！！");
                                    synchronized(_db){
                                      _db.execute(String.format("insert into t_result_ugc_comment_antispam (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
                                                ,commenterId, ip, token,  user_name, commentId,
                                                content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));
                                    }
                                    labelResult = "这是条广告！！";
                                }else
                                {
                                    if(isSpamerId(commenterId)&!isNotSpamerId(commenterId,entityId)&isComment2Many(commenterId))  // 如果是高频评论且不是狂热粉丝
                                    {
                                        System.out.println("这是高频疑似留言");
                                        synchronized(_db){
                                            _db.execute(String.format("insert into t_result_ugc_comment_antispam_highpv (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
                                                    ,commenterId, ip, token,  user_name, commentId,
                                                    content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));
                                            if(userLevel.equals("0"))
                                            {
                                                _db.execute(String.format("insert into t_result_ugc_comment_antispam (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
                                                        ,commenterId, ip, token,  user_name, commentId,
                                                        content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));
                                            }
                                        }
                                        labelResult = "这是高频疑似留言";
                                    }else
                                    {
                                        System.out.println("这是正常留言");
                                        labelResult = "这是正常留言";
                                    }
                                }
                                System.out.println(result.shortValue());
                            }
                            resultList.add(content+"    "+labelResult+" "+commenterId+" "+ip+" "+token+" "+user_name);
                            resultRDD =    ctx.parallelize(resultList,1);
                            resultRDD.saveAsTextFile("/ugc_result/"+timestamp);
                        }
                       }
                      catch(Exception e)
                      {
                          e.printStackTrace();
                      }
                }
                return null;
            }
        });
        jssc.start();
        jssc.awaitTermination();
    }
}
