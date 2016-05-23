package com.youku.ddshow.antispam.utils;

import com.youku.ddshow.antispam.ugc.bayes.ml.BayesKafkaWorkStreamV2;

import java.io.*;
import java.util.*;

/**
 * Created by dongjian on 2016/5/23.
 * 过滤昵称
 */
public class NickNameFilter {
    public static Map<String,String> spamNickNameMap = null;
    public static Set<String>  spamNickNameKeyWord =null;
    public static Map<String,String>  initspamNickNameMap() throws IOException
    {
        if(spamNickNameMap==null)
        {
            Map<String,String>  map = new HashMap<String,String>();
            spamNickNameKeyWord = new HashSet<String>();
            loadspamNickNameDic(map);
            spamNickNameMap = map;
            System.out.println("加载昵称替换词典：spamNickName.dic");
            return spamNickNameMap;
        }else
        {
            return spamNickNameMap;
        }
    }

    public static  void  loadspamNickNameDic(Map<String,String>  spamNickNameMap) throws IOException {
        InputStream in = Database.class.getClassLoader().getResourceAsStream(
                "spamNickName.dic");
        BufferedReader in2=new BufferedReader(new InputStreamReader(in));
        String y="";
        while((y=in2.readLine())!=null){//一行一行读
            spamNickNameMap.put(y.split(" ")[0],y.split(" ")[1]);
            spamNickNameKeyWord.add(y.split(" ")[1]);
        }
    }

    public static String relaceFilter(String nickName)
    {
        if(spamNickNameMap!=null)
        {
            Iterator it =  spamNickNameMap.keySet().iterator();
            while (it.hasNext())
            {
                String key = it.next().toString();
                String value = spamNickNameMap.get(key);
                nickName = nickName.replace(key,value);
            }
        }
        return nickName;
    }

    public static  String nickNameFilter(String nickName) throws IOException {
          initspamNickNameMap();
          return  relaceFilter(nickName);
    }

    public static  boolean isSpamNickName(String nickName)  throws IOException
    {
        String  filtedNickName = nickNameFilter(LaifengWordAnalyzer.relaceFilter(nickName));
        String  pinyinNickName = "";
        try{
            pinyinNickName = ChineseToEnglish.getPingYin(LaifengWordAnalyzer.relaceFilter(nickName));
        }
        catch (Exception e)
        {
             e.printStackTrace();
        }

        Iterator it = spamNickNameKeyWord.iterator();
        while (it.hasNext())
        {
            String spam = it.next().toString();
            if(filtedNickName.contains(spam)||pinyinNickName.contains(spam))
            {
                return  true;
            }
        }
        return  false;
    }

    public  static  void main(String[] args) throws IOException
    {
       /* try {
                String encoding="UTF-8";
                 File file=new File("D:\\文本分类\\nick_name.txt");
                if(file.isFile() && file.exists()){ //判断文件是否存在
                 InputStreamReader read = new InputStreamReader(
                 new FileInputStream(file),encoding);//考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            while((lineTxt = bufferedReader.readLine()) != null){

               if(isSpamNickName(lineTxt))
                 {
                    System.out.println(lineTxt);
                 }
                }
            read.close();
            }else{
            System.out.println("找不到指定的文件");
            }
        } catch (Exception e) {
        System.out.println("读取文件内容出错");
        e.printStackTrace();
        }*/
        System.out.println( BayesKafkaWorkStreamV2.isNewUser("810007157"));
      System.out.println( BayesKafkaWorkStreamV2.isNewUser("asdfasd"));
        System.out.println( BayesKafkaWorkStreamV2.isNewUser("910007157"));
    }
}
