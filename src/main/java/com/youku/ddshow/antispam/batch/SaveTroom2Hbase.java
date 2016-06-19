package com.youku.ddshow.antispam.batch;

import com.google.common.collect.Lists;
import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.HbaseUtils;
import groovy.lang.Tuple;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import com.alibaba.fastjson.JSON;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * Created by dongjian on 2016/6/13.
 * 把t_room 的内容定期存储到hbase里面
 */
public class SaveTroom2Hbase {
    private static final Pattern SPACE = Pattern.compile("\t");
    public static void main(String[] args)
    {
        if (args.length < 2) {
            System.err.println("Usage: SaveTroom2Hbase <master> <file>");
            System.exit(1);
        }

        JavaSparkContext ctx = new JavaSparkContext(args[0], "SaveTroom2Hbase",
                System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(SaveTroom2Hbase.class));
       // JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> line =  ctx.textFile(args[1],1);

        JavaRDD<ArrayList<String>> word =  line.map(new Function<String, ArrayList<String>>() {
            @Override
            public ArrayList<String> call(String s) {
                return Lists.newArrayList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, String> uidRoom =     word.mapToPair(new PairFunction<ArrayList<String>, String, String>() {
            @Override
            public Tuple2<String,String> call (ArrayList<String> strings)
            {
                String uid = "1";
                String roomId = "1";
                if(strings.size()>6)
                {
                    uid =  strings.get(1);
                    roomId = strings.get(0);
                }
                return  new Tuple2<String,String>(uid,roomId);
            }
        });

        uidRoom.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._1()+"-"+stringStringTuple2._2());
                //concurrentHashMap.put(stringStringTuple2._1(),stringStringTuple2._2());
            }
        });

        System.exit(0);
    }
}
