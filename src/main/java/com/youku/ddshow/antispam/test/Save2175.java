package com.youku.ddshow.antispam.test;

import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.CalendarUtil;
import com.youku.ddshow.antispam.utils.Database;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jian.dong on 2016/6/26.
 */
public class Save2175 {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static Database _db = null;
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: WordCount <master> <file>");
            System.exit(1);
        }
        _db  =  new Database(PropertiesType.DDSHOW_STAT_ONLINE);
        // com.youku.data.security.SecurityUtil.login();
        JavaSparkContext ctx = new JavaSparkContext(args[0], "JavaWordCount",
                System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(WordCount.class));
        JavaRDD<String> lines = ctx.textFile(args[1], 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) {
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<?,?> tuple : output) {
            System.out.println(tuple._1 + ": " + tuple._2);
        }

        synchronized(_db){
            _db.execute(String.format("insert into t_result_ugc_comment_antispam_highpv (commenterId,ip,device_token,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s','%s','%s');"
                    ,"123", "10.10.10.10", "sfsdfsfasfas",  "online"+"_test", "456",
                    "hahah", CalendarUtil.getDetailDateFormat(1466904022000L),"0"));

        }
        System.exit(0);
    }
}
