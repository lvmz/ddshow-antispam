/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.youku.ddshow.antispam.ugc.bayes.history;

import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.utils.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public final class FilterHistoryUgcCommentBatch {
  private static final Pattern SPACE = Pattern.compile(",");
  private static Database _db = null;
  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      System.err.println("Usage: FilterHistoryUgcComment <file> <modelPath>");
      System.exit(1);
    }
    _db  =  new Database(PropertiesType.DDSHOW_STAT_TEST);
     SparkConf sparkConf = new SparkConf().setAppName("FilterHistoryUgcComment").setExecutorEnv("file.encoding","UTF-8").setMaster("local");
     JavaSparkContext ctx = new JavaSparkContext(sparkConf);
     SQLContext sqlContext = new SQLContext(ctx);
    String modelPath = args[1];
     NaiveBayesModel loadedModel =     NaiveBayesModel.load(ctx.sc(),modelPath);

     JavaRDD<String> lines = ctx.textFile(args[0], 1);

     List<String> list = lines.toArray();
     for( String str : list)
     {
       //System.out.println(str);
       List<String> splitedList = new ArrayList<String>();
       splitedList = Arrays.asList(SPACE.split(str));
       if(splitedList.size()==6)
       {
         String commentId = splitedList.get(0);
         String commenterId = splitedList.get(1);
         String userLevel = splitedList.get(2);
         String user_name = splitedList.get(3);
         String timestamp = splitedList.get(4);
         String content = splitedList.get(5);
         List<String>   listForModel = new ArrayList<String>();
         listForModel.add(LaifengWordAnalyzer.wordAnalyzer(content));
         System.out.println(commentId+"这是分词结果：---》"+LaifengWordAnalyzer.wordAnalyzer(content));
         DataFrame rescaledData =   BysJava2.String2DataFrame(listForModel, ctx,sqlContext);

         if(rescaledData!=null)
         {
           String labelResult = "";
           List<String> resultList = new ArrayList<String>();
           for (Row r : rescaledData.select("features", "label").take(1)) {

             Vector features = r.getAs(0);
             Integer label =  r.getInt(1);

             Double result =  loadedModel.predict(features);
             if(result.shortValue()==0)
             {
               System.out.println(commentId+"这是条广告！！--------->"+content);
               labelResult = "这是条广告！！";
               synchronized(_db){
                 _db.execute(String.format("insert into t_result_ugc_comment_historyspam (commenterId,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s');"
                         ,commenterId,user_name, commentId,
                         content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));
               }
             }else
             {
               System.out.println(commentId+"这是正常留言！！--------->"+content);
               synchronized(_db){
                 _db.execute(String.format("insert into t_result_ugc_comment_historyspam_fail (commenterId,user_name,commentId,content,stat_time,user_level) values ('%s','%s','%s','%s','%s','%s');"
                         ,commenterId,user_name, commentId,
                         content,CalendarUtil.getDetailDateFormat(Long.parseLong(timestamp)),userLevel));
               }
             }
           }

         }
       }
     }

   /* JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
      @Override
      public Iterable<String> call(String s) throws IOException {
        List<String> splitedList = Arrays.asList(SPACE.split(s));

        if(splitedList.size() == 6)
        {
          String commentID = splitedList.get(0);
          String commenterID = splitedList.get(1);
          String userLevel = splitedList.get(2);
          String nickName = splitedList.get(3);
          String createTime = splitedList.get(4);
          String content = splitedList.get(5);
          List<String>   listForModel = new ArrayList<String>();
          listForModel.add(LaifengWordAnalyzer.wordAnalyzer(content));
          System.out.println(commentID+" "+commenterID+" "+userLevel+" "+nickName+" "+createTime+" "+content);

          ctx.parallelize(rowList);
        }
       // System.out.println("splitedList.size()------------>"+splitedList.size());
        return Arrays.asList(SPACE.split(s));
      }
    });
    words.count();*/
/*    System.out.println("words.count();------------>"+words.count());

    List<String> list =   lines.toArray();
    System.out.println("list.size()------------>"+list.size());
    for(String str : list)
    {
      List<String> splitedList = Arrays.asList(SPACE.split(str));
      String commentID = splitedList.get(0);
      String commenterID = splitedList.get(1);
      String userLevel = splitedList.get(2);
      String nickName = splitedList.get(3);
      String createTime = splitedList.get(4);
      String content = splitedList.get(5);
      System.out.println(commentID+" "+commenterID+" "+userLevel+" "+nickName+" "+createTime+" "+content);
    }*/
    ctx.stop();
  }
}
