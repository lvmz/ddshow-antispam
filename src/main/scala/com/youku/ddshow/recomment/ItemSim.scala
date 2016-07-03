package com.youku.ddshow.recomment

import breeze.numerics.sqrt
import org.apache.spark.rdd.RDD
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

    val outputpath = "D:\\推荐\\output2"

    //   #1 读取数据，从HDFS上
    val conf = new SparkConf().setAppName("ItemSim").setMaster("local")
    val sc = new SparkContext(conf)
    val user_rdd2 =     UserData(sc,inputpath,",")
    val user_rdd12 = Cooccurrence(user_rdd2)
    val rdd_app1_R7 =   Recommend(user_rdd12,user_rdd2,2)
    rdd_app1_R7.saveAsTextFile(outputpath)

    /*val user_rdd1 = sc.textFile(inputpath, 5)

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

      user_rdd12.saveAsTextFile(outputpath)*/
     //  Recommend(user_rdd12,user_rdd2,2)
  }

  def UserData (

    sc:SparkContext,input:String,

    split:String

  ):(RDD[(String,String,Double)]) = {

    val user_rdd1= sc.textFile(input,10)

    val user_rdd2=user_rdd1.map(line=> {

      val fileds= line.split(split)

      (fileds(0),fileds(1),fileds(2).toDouble)

    })
    user_rdd2
  }

  def Cooccurrence (

                     user_rdd:RDD[(String,String,Double)]

                   ) : (RDD[(String,String,Double)]) = {

    //  0 数据做准备

    val user_rdd2=user_rdd.map(f => (f._1,f._2)).sortByKey()

    user_rdd2.cache

    //  1 (用户：物品)笛卡尔积 (用户：物品) =>物品:物品组合

    val user_rdd3=user_rdd2.join(user_rdd2)

    val user_rdd4=user_rdd3.map(data=> (data._2,1))

    //  2 物品:物品:频次

    val user_rdd5=user_rdd4.reduceByKey((x,y) => x + y)

    //  3 对角矩阵

    val user_rdd6=user_rdd5.filter(f=> f._1._1 == f._1._2)

    //  4 非对角矩阵

    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)

    //  5 计算同现相似度（物品1，物品2，同现频次）

    val user_rdd8=user_rdd7.map(f=> (f._1._1, (f._1._1, f._1._2, f._2))).

      join(user_rdd6.map(f=> (f._1._1, f._2)))

    val user_rdd9=user_rdd8.map(f=> (f._2._1._2, (f._2._1._1,

      f._2._1._2, f._2._1._3, f._2._2)))

    val user_rdd10=user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))

    val user_rdd12=user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))

    //   6结果返回

    user_rdd12

  }

  def CosineSimilarity (

    user_rdd:RDD[(String,String,Double)]

  ) : (RDD[(String,String,Double)]) = {

    //  0 数据做准备

    val user_rdd2=user_rdd.map(f => (f._1,(f._2,f._3))).sortByKey()

    user_rdd2.cache

    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合

    val user_rdd3=user_rdd2.join(user_rdd2)

    val user_rdd4=user_rdd3.map(f=> ((f._2._1._1, f._2._2._1),(f._2._1._2, f._2._2._2)))

    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1*评分2）组合并累加

    val user_rdd5=user_rdd4.map(f=> (f._1,f._2._1*f._2._2 )).reduceByKey(_+_)

    //  3 对角矩阵

    val user_rdd6=user_rdd5.filter(f=> f._1._1 == f._1._2)

    //  4 非对角矩阵

    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)

    //  5 计算相似度

    val user_rdd8=user_rdd7.map(f=> (f._1._1, (f._1._1, f._1._2, f._2))).

      join(user_rdd6.map(f=> (f._1._1, f._2)))

    val user_rdd9=user_rdd8.map(f=> (f._2._1._2, (f._2._1._1,

      f._2._1._2, f._2._1._3, f._2._2)))

    val user_rdd10=user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))

    val user_rdd11 = user_rdd10.map(f => (f._2._1._1,f._2._1._2,f._2._1._3,f._2._1._4,f._2._2))

    val user_rdd12=user_rdd11.map(f=> (f._1, f._2, (f._3 / sqrt(f._4 * f._5)) ))

    //  7 结果返回

    user_rdd12

  }

  def EuclideanDistanceSimilarity (

                                    user_rdd:RDD[(String,String,Double)]

                                  ) : (RDD[(String,String,Double)]) = {

    //  0 数据做准备

    val user_rdd2=user_rdd.map(f => (f._1,(f._2,f._3))).sortByKey()

    user_rdd2.cache

    //  1 (用户,物品,评分)笛卡尔积 (用户,物品,评分) =>（物品1,物品2,评分1,评分2）组合

    val user_rdd3=user_rdd2.join(user_rdd2)

    val user_rdd4=user_rdd3.map(f=> ((f._2._1._1, f._2._2._1),(f._2._1._2, f._2._2._2)))

    //  2 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,评分1-评分2）组合并累加

    val user_rdd5=user_rdd4.map(f=> (f._1,(f._2._1-f._2._2 )*(f._2._1-f._2._2 ))).reduceByKey(_+_)

    //  3 （物品1,物品2,评分1,评分2）组合 => （物品1,物品2,1）组合并累加   计算重叠数

    val user_rdd6=user_rdd4.map(f=> (f._1,1)).reduceByKey(_+_)

    //  4 非对角矩阵

    val user_rdd7=user_rdd5.filter(f=> f._1._1 != f._1._2)

    //  5 计算相似度

    val user_rdd8=user_rdd7.join(user_rdd6)

    val user_rdd9=user_rdd8.map(f=> (f._1._1,f._1._2,f._2._2/(1+sqrt(f._2._1))))

    //   7 结果返回

    user_rdd9

  }

  def Recommend (

                  items_similar:RDD[(String,String,Double)],

                  user_perf:RDD[(String,String,Double)],

                  r_number:Int

                ) :(RDD[(String,String,Double)]) = {

    //   1 矩阵计算——i行与j列join

    val rdd_app1_R2 = items_similar.map(f => (f._2, (f._1,f._3))).

      join(user_perf.map(f => (f._2,(f._1,f._3))))

    //   2 矩阵计算——i行与j列元素相乘

    val rdd_app1_R3 = rdd_app1_R2.map(f => ((f._2._2._1,f._2._1._1),f._2._2._2*f._2._1._2))

    //   3 矩阵计算——用户：元素累加求和

    val rdd_app1_R4 = rdd_app1_R3.reduceByKey((x,y) => x+y).map(f=> (f._1._1,(f._1._2,f._2)))

    //   4 矩阵计算——用户：用户对结果排序，过滤

    val rdd_app1_R5 = rdd_app1_R4.groupByKey()

    val rdd_app1_R6 = rdd_app1_R5.map(f => {

      val i2 = f._2.toBuffer

      val i2_2 = i2.sortBy(_._2)

      if (i2_2.length > r_number)i2_2.remove(0,(i2_2.length-r_number))

      (f._1,i2_2.toIterable)

    })
    val rdd_app1_R7 = rdd_app1_R6.flatMap(f => {

      val id2 = f._2

      for (w <- id2 ) yield (f._1,w._1,w._2)

    })
    rdd_app1_R7
  }
}
