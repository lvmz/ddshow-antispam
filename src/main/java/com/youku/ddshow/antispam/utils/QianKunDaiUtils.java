package com.youku.ddshow.antispam.utils;

import com.youku.ddshow.antispam.model.QianKunDai.*;
import com.alibaba.fastjson.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by caixiaojun on 2016/7/6.
 */
public class QianKunDaiUtils {
    private static final Logger logger = LoggerFactory.getLogger(QianKunDaiUtils.class);

    private static final int TIME_OUT=50000;

    private final static String  KV_GET="/get";

    private final static String  KV_SET="/set";

    private final static String KV_DEL="/del";

    private final static String KV_TTL="/ttl";

    private final static  String KV_INCR="/incr";

    private final static  String KV_INCRBY="/incrBy";

    private final static  String KV_MGET="/mget";

    private final static  String KV_MSET="/mset";

    /**
     * kv/get
     * @param url
     * @param parameter
     * @return
     */
    public static QkdKVGetResult kvGet(String url, QkdParameter parameter){
        QkdKVGetResult qkdResult=new QkdKVGetResult();
        try {
            String result = post(url+KV_GET, JSON.toJSONString(parameter));
            qkdResult = JSON.parseObject(result,QkdKVGetResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  qkdResult;
        }

    }

    /**
     * kv/set
     * @param url
     * @param parameter
     * @return
     */
    public static QkdResult kvSet(String url, QkdParameterDetail parameter){
        QkdResult setResult=new QkdResult();
        try {
            String result = post(url+KV_SET, JSON.toJSONString(parameter));
            setResult = JSON.parseObject(result,QkdResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  setResult;
        }
    }

    /**
     * kv/del
     * @param url
     * @param parameter
     * @return
     */
    public static QkdResult kvDel(String url,QkdParameter parameter){
        QkdResult delResult=new QkdResult();
        try {
            String result = post(url+KV_DEL, JSON.toJSONString(parameter));
            delResult =  JSON.parseObject(result,QkdResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  delResult;
        }
    }




 /*   public static kvTtl(String url,QkdParameter parameter){
        QkdResult ttlResult=new QkdResult();
        try {
            String result = post(url+KV_TTL, JSON.toJSONString(parameter));
            JSON.parseObject(result,QkdResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  ttlResult;
        }
    }
*/

 /*   public static QkdResult kvIncr(String url, QkdParameter parameter){
        QkdResult qkdResult=new QkdResult();
        try {
            String result = post(url+KV_INCR, JSONUtil.oject2Json(parameter));
            qkdResult=JSONUtil.json2Object(result, QkdResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  qkdResult;
        }
    }*/


 /*   public static QkdResult kvIncrBy(String url, QkdParameterDetail parameter){
        QkdResult qkdResult=new QkdResult();
        try {
            String result = post(url+KV_INCRBY, JSONUtil.oject2Json(parameter));
            qkdResult=JSONUtil.json2Object(result, QkdResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  qkdResult;
        }
    }*/


    public static QkdMgetResult kvMget(String url, QkdMgetParameter parameter){
        QkdMgetResult qkdMesult=new QkdMgetResult();
        try {
            String result = post(url+KV_MGET, JSON.toJSONString(parameter));
            System.out.println(result);
            qkdMesult=JSON.parseObject(result, QkdMgetResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  qkdMesult;
        }
    }


   /* public static QkdMsetResult kvMset(String url, QKdMsetParameter parameter){
        QkdMsetResult qkdMsesult=new QkdMsetResult();
        try {
            String result = post(url+KV_MSET, JSONUtil.oject2Json(parameter));
            qkdMsesult=JSONUtil.json2Object(result, QkdMsetResult.class);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(),e);
        }
        finally {
            return  qkdMsesult;
        }
    }*/


    public static String post(String url,String jsonParam) throws Exception {
        HttpClient client = new DefaultHttpClient();
        client.getParams().setParameter("http.socket.timeout", TIME_OUT);
        client.getParams().setParameter("http.connection.timeout", TIME_OUT);
        HttpPost post = new HttpPost(url);
        StringEntity s = new StringEntity(jsonParam, "UTF-8");
        s.setContentType("application/json");
        post.setEntity(s);
        HttpResponse response = client.execute(post);
        InputStream in = response.getEntity().getContent();
        BufferedReader rd = new BufferedReader(new InputStreamReader(in, "UTF-8"));
        String line;
        StringBuilder builder = new StringBuilder();
        while ((line = rd.readLine()) != null) {
            builder.append(line);
        }
        rd.close();
        return builder.toString();

    }

    public static void main(String[] args) {
        String url="http://10.100.23.57:80/hdp/kvstore/kv/";
        int length=1000;
      /*  QkdParameter parameter=new QkdParameter();
        parameter.setAk("CuVEz/TxDu");
        parameter.setK("spamWordKey");
        parameter.setUri("qkd://BJTEST/6j");

        QkdParameterDetail parameterDetail=new QkdParameterDetail();
        parameterDetail.setAk("CuVEz/TxDu");
        parameterDetail.setK("spamWordKey");
        parameterDetail.setUri("qkd://BJTEST/6j");
        parameterDetail.setV("18010069603"+","+"842307"+","+"283248744");

        QkdParameter parameter1=new QkdParameter();
        parameter1.setAk("CuVEz/TxDu");
        parameter1.setK("spamWordKey");
        parameter1.setUri("qkd://BJTEST/6j");


        QkdParameterDetail parameterDetail1=new QkdParameterDetail();
        parameterDetail1.setAk("CuVEz/TxDu");
        parameterDetail1.setK("spamWordKey");
        parameterDetail1.setUri("qkd://BJTEST/6j");
        parameterDetail1.setV("10");

       //kvSet(url,parameterDetail);
        System.out.println(""+  kvGet(url,parameter).getV());
       // kvDel(url,parameter);
        System.out.println(""+ kvGet(url,parameter).getV());*/
 /*       kvSet(url,parameterDetail);
        System.out.println(""+kvTtl(url,parameter).getData());*/
      //  System.out.println(""+kvIncr(url,parameter1).getData());
      //  System.out.println(""+kvIncrBy(url,parameterDetail1).getData());



      /*  QKdMsetParameter msetParameter=new QKdMsetParameter();
        msetParameter.setAk("CuVEz/TxDu");
        msetParameter.setUri("qkd://BJTEST/6j");
        List<Entity> entities=new ArrayList<>();
        for(int i=0;i<length;i++){
            Entity entity=new Entity();
            entity.setK("key"+(i));
            entity.setV(i+"");
            entities.add(entity);
        }
        msetParameter.setKs(entities);*/

        QkdMgetParameter msgtParameter=new QkdMgetParameter();
        msgtParameter.setAk("CuVEz/TxDu");
        msgtParameter.setUri("qkd://BJTEST/6j");
        List<Key> ks=new ArrayList<>();
        for(int i=10;i<length;i++){
            Key key=new Key();
            key.setK("laifeng_spamwordkey"+"_"+(i));
            ks.add(key);
        }
        msgtParameter.setKs(ks);


       // QkdMsetResult qkdMsetResult=kvMset(url,msetParameter);
      //  System.out.println("mset执行时间:"+qkdMsetResult.getCost());
        QkdMgetResult qkdMgetResult=kvMget(url,msgtParameter);

        System.out.println("mget执行时间:"+qkdMgetResult.getCost());

    }

}
