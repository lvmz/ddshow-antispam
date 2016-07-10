package com.youku.ddshow.antispam.streaming.ugc;

import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.model.QianKunDai.QkdParameter;
import com.youku.ddshow.antispam.utils.Database;
import com.youku.ddshow.antispam.utils.QianKunDaiUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
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
 * Created by jian.dong on 2016/7/10.
 */
public class TestQkd {
    private static final Pattern SPACE = Pattern.compile("\t");
    private static final String url="http://10.100.23.57:80/hdp/kvstore/kv/";
    public static void main(String[] args) {
        //************************************线上用**************************************************
        if (args.length < 5) {
            System.err.println("Usage: UgcCommentAntiSpam <token> <group> <topics> <numThreads> <master> <dutationg>");
            System.exit(1);
        }


        final  Long dutationg = Long.parseLong(args[5]);
        SparkConf sparkConf = new SparkConf().setAppName("UgcCommentAntiSpam").setExecutorEnv("file.encoding","UTF-8");
        // Create the context with 60 seconds batch size

        final JavaStreamingContext jssc = new JavaStreamingContext(args[4],"UgcCommentAntiSpam", new Duration(dutationg),System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(UgcCommentAntiSpamByContent.class));


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
                QkdParameter parameter=new QkdParameter();
                parameter.setAk("CuVEz/TxDu");
                parameter.setK("caixiaojun");
                parameter.setUri("qkd://LIHANGTEST/6j");
                System.out.println(""+  QianKunDaiUtils.kvGet(url,parameter).getV());
                return Lists.newArrayList(SPACE.split(s));
            }
        });
        splited.count();
        jssc.start();
        jssc.awaitTermination();
    }
}
