package com.youku.ddshow.antispam.test;

/**
 * Created by dongjian on 2016/7/4.
 */
public class CreateMatricsForRecomment {
    public static  int userNum = 5; //用户数
    public static  int itemNum = 5;//物品数，抽象概念，也可以是主播数
    public static String split = ",";
    public static void createUserScoreMatrics()
    {
        for(int i=0;i<userNum;i++)
        {
            for(int j=0;j<itemNum;j++)
            {
                Double dd = Math.random()*10;
                if(dd.intValue()%3==0)
                {
                   // System.out.println(i+split+j+split+" ");
                }else
                {
                    System.out.println(i+split+j+split+Math.random());
                }

            }
        }
    }

    public static void createUserClickMatrics()
    {
        for(int i=0;i<userNum;i++)
        {
            for(int j=0;j<itemNum;j++)
            {
                Double dd = Math.random()*10;
                if(dd.intValue()%3==0)
                {
                    //System.out.println(i+split+j+split+" ");
                }else
                {
                    System.out.println(i+split+j+split+dd.intValue());
                }

            }
        }
    }
       public static void main(String[] args)
       {
           createUserClickMatrics();
           createUserClickMatrics();
       }
}
