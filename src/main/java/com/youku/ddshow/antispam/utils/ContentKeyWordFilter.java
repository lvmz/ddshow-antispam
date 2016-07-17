package com.youku.ddshow.antispam.utils;


import com.youku.ddshow.antispam.model.PropertiesType;
import com.youku.ddshow.antispam.model.QianKunDai.QkdParameter;
import com.youku.ddshow.antispam.model.QianKunDai.QkdParameterDetail;

import java.io.*;
import java.util.*;

/**
 * Created by dongjian on 2016/5/23.
 * 过滤昵称
 */
public class ContentKeyWordFilter implements Serializable {
    public static Map<String,String> spamNickNameMap = null;
    public static Set<String>  spamNickNameKeyWord =null;
    private Properties props = new Properties();
    public ContentKeyWordFilter(PropertiesType type) throws IOException {
        InputStream in = Database.class.getClassLoader().getResourceAsStream(
                type.getValue());
        props.load(in);
        in.close();
    }
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
                "contentSpamKeyWord.dic");
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

    public   boolean isSpamNickName(String nickName)  throws IOException
    {
        String  filtedNickName = nickNameFilter(LaifengWordAnalyzer.relaceFilter(LaifengWordAnalyzer.StringFilter(nickName)));
       // System.out.println("LaifengWordAnalyzer.StringFilter "+LaifengWordAnalyzer.StringFilter(nickName));
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

        Set redisSet = getSetFromQkd();
        if(redisSet!=null)
        {
            Iterator it2 = redisSet.iterator();
            while (it2.hasNext())
            {
                String spam = it2.next().toString();
                if(filtedNickName.contains(spam)||pinyinNickName.contains(spam))
                {
                    return  true;
                }
            }
        }

        return  false;
    }
     public  Set getSetFromQkd()
    {

        if(props!=null)
       {
           HashSet set = new HashSet();
           String url=props.getProperty("url");
           QkdParameter parameter=new QkdParameter();
           parameter.setAk(props.getProperty("appKey"));
           parameter.setK(props.getProperty("spamWordKey"));
           parameter.setUri(props.getProperty("uri"));
           String value = QianKunDaiUtils.kvGet(url,parameter).getV();
           setQkdValue(value);
           String[] valueArray =    value.split(",");
           for(String keyWord:valueArray)
           {
               set.add(keyWord);
           }
          /// System.out.println(""+  QianKunDaiUtils.kvGet(url,parameter).getV());
           return set;
       }else
       {
           return null;
       }
    }
    private  void setQkdValue(String value)
    {
        String url=props.getProperty("url");
        QkdParameterDetail parameterDetail=new QkdParameterDetail();
        parameterDetail.setAk(props.getProperty("appKey"));
        parameterDetail.setK(props.getProperty("spamWordKey"));
        parameterDetail.setUri(props.getProperty("uri"));
        parameterDetail.setV(value);
        QianKunDaiUtils.kvSet(url,parameterDetail);
    }

    public  void saveSpam2Qkd(String value,Integer keyNum)
    {
        String url=props.getProperty("url");
        QkdParameterDetail parameterDetail=new QkdParameterDetail();
        parameterDetail.setAk(props.getProperty("appKey"));
        parameterDetail.setK(props.getProperty("saveSpamWordKey")+"_"+keyNum);
        System.out.println("key------------>"+props.getProperty("saveSpamWordKey")+"_"+keyNum);
        parameterDetail.setUri(props.getProperty("uri"));
        QkdParameter parameter=new QkdParameter();
        parameter.setAk(props.getProperty("appKey"));
        parameter.setK(props.getProperty("saveSpamWordKey")+"_"+keyNum);
        parameter.setUri(props.getProperty("uri"));
        String valueold = QianKunDaiUtils.kvGet(url,parameter).getV();
        if(valueold!=null)
        {
            if(!valueold.contains(value))
            {
                valueold=valueold+"_"+value;
                parameterDetail.setV(valueold);

            }
        }else
        {
            parameterDetail.setV(value);
        }
        QianKunDaiUtils.kvSet(url,parameterDetail);
    }

    public  static  void main(String[] args) throws IOException
    {
      ContentKeyWordFilter contentKeyWordFilter =  new ContentKeyWordFilter(PropertiesType.DDSHOW_QKD_TEST);
        Long a = System.currentTimeMillis();
        System.out.println(contentKeyWordFilter.isSpamNickName("万部国.产日韩欧美制服动漫.看A.片.加微信 can664"));
        System.out.println(System.currentTimeMillis()-a);
       /* try {
                String encoding="UTF-8";
                 File file=new File("D:\\文本分类\\antispam_keyword.txt");
                if(file.isFile() && file.exists()){ //判断文件是否存在
                 InputStreamReader read = new InputStreamReader(
                 new FileInputStream(file),encoding);//考虑到编码格式
            BufferedReader bufferedReader = new BufferedReader(read);
            String lineTxt = null;
            while((lineTxt = bufferedReader.readLine()) != null){

               if(!contentKeyWordFilter.isSpamNickName(lineTxt))
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
    }
}
