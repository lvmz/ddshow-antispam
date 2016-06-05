package com.youku.ddshow.antispam.utils;


import java.io.Serializable;
import java.util.*;

/**
 * Created by dongjian on 2016/6/1.
 */
public  class  Utils implements Serializable {
    /**
     * 把合并的字符分隔为按时间戳倒序排列的map
     * @param reducedStr
     * @return
     */
    public static Map<Long,Integer> str2Map(String reducedStr)
    {
        Map<Long,Integer> map = new TreeMap<Long,Integer>();
        if(reducedStr.indexOf("-")==-1) return  null;

        String[] s1 =  reducedStr.split("-");
        for(int i=0;i<s1.length;i++)
        {
            String[] s2=   s1[i].split("_");
            map.put(Long.parseLong(s2[0]),Integer.parseInt(s2[1]));
        }
        return map;
    }

    public  static Map<Long,Long> Map2KMap(Map<Long,Integer> map)
    {
        if(map==null) return null;
        Iterator<Long> it =  map.keySet().iterator();
        Long big = null;
        Long small = null;
        int i = 0;
        List<Long> list = new ArrayList<Long>();
        Map<Long,Long> map2 = new TreeMap<Long,Long>();
        while (it.hasNext())
        {
            if(i==0)
            {
                small = it.next();
            }
            if(i==1)
            {
                big = it.next();
            }
            if(big!=null&&small!=null&&i>1)
            {
                Long timeIntervel = (big - small)/1000;
                Integer popular = map.get(big) - map.get(small);
                Long k = popular/timeIntervel;
                map2.put(big,k);
                list.add(k);
                small = big;
                big = it.next();
            }
            i++;
        }
        return  map2;
    }
    public static  void main(String[] args)
    {
       String reduce = "1464764689000_2837-1464764657000_2797-1464764838000_3014-1464764728000_2876-1464764902000_3096-1464764771000_2932-1464764828000_2998-1464764852000_3030-1464764864000_3051-1464764876000_3066";
        String reduce2 = "1464764689000_2837";

        Map map =    str2Map(reduce);
        Map map2 = Map2KMap(map);

       System.out.println();
    }
}
