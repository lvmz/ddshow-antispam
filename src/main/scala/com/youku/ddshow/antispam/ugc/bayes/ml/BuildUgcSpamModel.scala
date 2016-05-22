package com.youku.ddshow.antispam.ugc.bayes.ml

import org.apache.spark.ml.feature.{IDF, HashingTF, Tokenizer}
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.Row
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by dongjian on 2016/4/28.
 * 构建ugc反垃圾模型
 */
object BuildUgcSpamModel {
   case class RawDataRecord(category: String, text: String)
  def main(args : Array[String]){

    if (args.length < 2) {
      System.err.println("Usage: BuildUgcSpamModel <path of ugc_antispam_train_data> <path of ugcAntispamModel to save>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("BuildUgcSpamModel").setExecutorEnv("file.encoding","UTF-8").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._                               // 导入spark sql的内容

    var srcRDD = sc.textFile(args(0)).map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }

    //70%作为训练数据，30%作为测试数据
    val splits = srcRDD.randomSplit(Array(0.7, 0.3))
    //var trainingDF = splits(0).toDF() // 如果数据量很大，可以使用部分数据做模型，部分数据做训练
    var trainingDF = srcRDD.toDF() // 由于数据量较少，所以用全部数据做训练数据
    var testDF = splits(1).toDF()

    //将词语转换成数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    var wordsData = tokenizer.transform(trainingDF)

    //计算每个词在文档中的词频
    var hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("words").setOutputCol("rawFeatures")
    var featurizedData = hashingTF.transform(wordsData)

    //计算每个词的TF-IDF
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    var idfModel = idf.fit(featurizedData)
    var rescaledData = idfModel.transform(featurizedData)

    //转换成Bayes的输入格式
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //训练模型
    val model = NaiveBayes.train(trainDataRdd, lambda = 1.0, modelType = "multinomial")

    model.save(sc, args(1))

    //测试数据集，做同样的特征表示及格式转换
    var testwordsData = tokenizer.transform(testDF)
    var testfeaturizedData = hashingTF.transform(testwordsData)
    var testrescaledData = idfModel.transform(testfeaturizedData)
    var testDataRdd = testrescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }

    //对测试数据集使用训练模型进行分类预测
    val testpredictionAndLabel = testDataRdd.map(p => (model.predict(p.features), p.label))

    //统计分类准确率
    var testaccuracy = 1.0 * testpredictionAndLabel.filter(x => x._1 == x._2).count() / testDataRdd.count()

    println("正确分类概率：")
    println(testaccuracy)

  }
}
