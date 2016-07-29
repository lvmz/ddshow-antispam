package com.youku.ddshow.antispam.batch;

import com.youku.ddshow.antispam.utils.ContentKeyWordFilter;
import com.youku.ddshow.antispam.utils.LaifengWordAnalyzer;
import com.youku.ddshow.antispam.utils.SensitiveWord.SensitivewordFilter;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;


public final class FilterSensitiveNickName {
    private static final Pattern SPACE = Pattern.compile("\t");
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: FilterSensitiveNickName <master> <file>");
            System.exit(1);
        }

       // com.youku.data.security.SecurityUtil.login();
        JavaSparkContext ctx = new JavaSparkContext(args[0], "FilterSensitiveNickName",
                System.getenv("SPARK_HOME"), JavaSparkContext.jarOfClass(FilterSensitiveNickName.class));
        final  SensitivewordFilter filter = new SensitivewordFilter();
        final Broadcast<SensitivewordFilter> SensitivewordFilterBroadcast   =   ctx.broadcast(filter);
        final JavaRDD<String> lines = ctx.textFile(args[1], 1);
        lines.map(new Function<String, List>() {
            @Override
            public List call(String s) throws Exception {
                return Arrays.asList(SPACE.split(s));
            }
        }).filter(new Function<List, Boolean>() {
            @Override
            public Boolean call(List list) throws Exception {
                return list.size()>1;
            }
        }).mapToPair(new PairFunction<List, String, String>() {
            @Override
            public Tuple2<String, String> call(List list) throws Exception {
                return new Tuple2<String, String>(list.get(0).toString(),list.get(1).toString());
            }
        }).mapValues(new Function<String, Tuple2<String,Set>>() {
            @Override
            public Tuple2<String, Set> call(String s) throws Exception {
                SensitivewordFilter filter =  SensitivewordFilterBroadcast.getValue();
                return new Tuple2<String, Set>(s,filter.getSensitiveWord(LaifengWordAnalyzer.StringFilter(s),1));
            }
        }).filter(new Function<Tuple2<String, Tuple2<String, Set>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<String, Set>> stringTuple2Tuple2) throws Exception {
                return stringTuple2Tuple2._2()._2().size()>0;
            }
        }).map(new Function<Tuple2<String,Tuple2<String,Set>>, String>() {

            @Override
            public String call(Tuple2<String, Tuple2<String, Set>> stringTuple2Tuple2) throws Exception {
                StringBuilder stringbuider = new StringBuilder();
                stringbuider.append(stringTuple2Tuple2._1()).append("\t").append(stringTuple2Tuple2._2()._1());
                return null;
            }
        })

                .saveAsTextFile(args[2]);

          /*      .foreach(new VoidFunction<Tuple2<String, Tuple2<String, Set>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<String, Set>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2._1()+" "+stringTuple2Tuple2._2()._1()+" "+stringTuple2Tuple2._2()._2().size());
            }
        });*/
        System.exit(0);
    }
}