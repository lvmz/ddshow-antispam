package com.youku.ddshow.recomment

import breeze.numerics.sqrt
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by jian.dong on 2016/7/3.
  *输入数据格式：

  * 用户ID，物品ID，评分

  * 输出结果格式：

  * 物品ID1，物品ID2，相似度
  */
object ItemSim {
  def main(args: Array[String]) {
    val inputpath = "D:\\推荐\\input"

    val outputpath = "D:\\推荐\\output"

    //   #1 读取数据，从HDFS上
    val conf = new SparkConf().setAppName("ItemSim").setMaster("local")
    val sc = new SparkContext(conf)
    val user_rdd1 = sc.textFile(inputpath, 5)

    //   #2 数据分割

    val user_rdd2 = user_rdd1.map(line => {

      val fileds = line.split(",")

      (fileds(0), fileds(1))

    }).sortByKey()

    user_rdd2.cache

    //   #3 (用户：物品) 笛卡尔积 (用户：物品)=> 物品:物品组合

    val user_rdd3 = user_rdd2 join user_rdd2

    val user_rdd4 = user_rdd3.map(data =>(data._2, 1))

    //   #4 物品:物品:频次

    val user_rdd5 = user_rdd4.reduceByKey((x,y) => x + y)

    //   #5 对角矩阵

    val user_rdd6 = user_rdd5.filter(f => f._1._1== f._1._2)

    //   #6 非对角矩阵

    val user_rdd7 = user_rdd5.filter(f => f._1._1!= f._1._2)

    //   #7 计算同现相似度（包名1，包名2，同现频次，包名1频次，包名2频次，相似度）

    val user_rdd8 = user_rdd7.map(f => (f._1._1,Array(f._1._1, f._1._2, f._2))).

      join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd9 = user_rdd8.map(f => (f._2._1(1).toString,Array(f._2._1(0).toString,

      f._2._1(1).toString, f._2._1(2), f._2._2)))

    val user_rdd10 = user_rdd9.join(user_rdd6.map(f=> (f._1._1, f._2)))

    val user_rdd11 = user_rdd10.map(f => (f._2._1(0).toString,f._2._1(1).toString, f._2._1(2).toString.toInt, f._2._1(3).toString.toInt, f._2._2))

    val user_rdd12 = user_rdd11.map(f => (f._1,f._2, (f._3 / sqrt(f._4 * f._5))))

      //   #8 结果输出HDFS

      user_rdd12.saveAsTextFile(outputpath)
  }
}
