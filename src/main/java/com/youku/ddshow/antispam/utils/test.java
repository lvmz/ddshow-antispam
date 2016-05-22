package com.youku.ddshow.antispam.utils;
import  com.alibaba.druid.util.Base64;
/**
 * Created by dongjian on 2016/5/5.
 */
public class test {
     public  static  void  main(String[] args)
     {
         byte[] b =   Base64.base64ToByteArray("UFzv4eyGVAGiskTXitOup8WI9nfppbYoWXxB6iVcdXWk3BJM+qfOkBNgDdEZ3yeYEyPWniw8hLcDG6koeO+Jqg==");
         System.out.println(b);
         String password = "happy";
         System.out.println(Base64.byteArrayToBase64(password.getBytes()));
     }
}
