package com.youku.ddshow.antispam.utils;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.wltea.analyzer.lucene.IKAnalyzer;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Created by dongjian on 2016/4/15.
 * 用来将来疯用户产生的内容进行分词
 */
public class LaifengWordAnalyzer {
     //public  static  Analyzer analyzer =  new IKAnalyzer(false);
     public static Map<String,String>  repalceMap = null;
     public static Map<String,String>  initrepalceMap() throws IOException
     {
         if(repalceMap==null)
         {
             Map<String,String>  map = new HashMap<String,String>();
             loadReplacedic(map);
             repalceMap = map;
             System.out.println("加载替换词典：replace.dic");
             return repalceMap;
         }else
         {
             return repalceMap;
         }
     }
     public static  void  loadReplacedic(Map<String,String>  repalceMap) throws IOException {
         InputStream in = Database.class.getClassLoader().getResourceAsStream(
                 "replace.dic");
         BufferedReader in2=new BufferedReader(new InputStreamReader(in));
         String y="";
         while((y=in2.readLine())!=null){//一行一行读
             repalceMap.put(y.split(" ")[0],y.split(" ")[1]);
         }
     }
     public static String relaceFilter(String userContent)
     {
         if(repalceMap!=null)
         {
             Iterator  it =  repalceMap.keySet().iterator();
             while (it.hasNext())
             {
                 String key = it.next().toString();
                 String value = repalceMap.get(key);
                 userContent = userContent.replace(key,value);
             }
         }
         return userContent;
     }
     public static  String wordAnalyzer(String userContent) throws IOException {
         initrepalceMap();
         Analyzer analyzer = new IKAnalyzer(false);
         userContent = relaceFilter(userContent);
         System.out.println(" 去除特殊字符结果 ："+StringFilter(userContent));
         StringReader reader = new StringReader(StringFilter(userContent));
         TokenStream ts = analyzer.tokenStream("", reader);
         CharTermAttribute term=ts.getAttribute(CharTermAttribute.class);
         StringBuilder stringBuilder = new StringBuilder();
         while(ts.incrementToken()){
             stringBuilder.append(term.toString()+" ");
         }
         analyzer.close();
         reader.close();
         return stringBuilder.toString();
     }
    public static String StringFilter(String str) throws PatternSyntaxException {
        String regEx="[`~～_!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*＊♬✘（）———+|{}【】\\-‘；：”“’。，、？0-9a-zA-Z]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }
    public  static void main(String[] args) throws IOException {
        System.out.println(LaifengWordAnalyzer.StringFilter("[亲亲][亲亲][亲亲]看-主-播_私_拍_加-薇-信 cv18988[害羞][害羞][微笑][微笑][鬼脸][鬼脸]"));

       // System.out.println(StringFilter("有要玩lou聊加我喔，171360q4982"));
       // Map<String,String>  map = new HashMap<String,String>();
        //loadReplacedic(map);
    }
}
