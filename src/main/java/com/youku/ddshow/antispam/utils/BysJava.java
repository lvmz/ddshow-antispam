package com.youku.ddshow.antispam.utils;
import java.util.ArrayList;
import java.util.Arrays;
import  java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;

/**
 * Created by dongjian on 2016/4/18.
 */
public class BysJava {
    public static void call(JavaReceiverInputDStream<String> lines)
    {
       System.out.println("1232312");
    }
    public  static  DataFrame String2DataFrame(List<String>  list,JavaSparkContext ctx)
    {
        if(list.size()<1) return null;
        list.add(" ");
        SQLContext sqlContext = new SQLContext(ctx);

        List<Row> rowList = new ArrayList<Row>();
        for(String line:list)
        {
            rowList.add(RowFactory.create(0, line));
        }
        JavaRDD<Row> jrdd = ctx.parallelize(rowList);
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });

        DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);

        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        DataFrame wordsData = tokenizer.transform(sentenceData);
        int numFeatures = 500000;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);
        DataFrame featurizedData = hashingTF.transform(wordsData);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);
        DataFrame rescaledData = idfModel.transform(featurizedData);
        return rescaledData;
    }
    public  static void main(String[] args)
    {

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        SQLContext sqlContext = new SQLContext(ctx);

        JavaRDD<Row> jrdd = ctx.parallelize(Arrays.asList(
                RowFactory.create(0, "加 维信 mkt858 mkt 858 笔 b 蓅 水 艳 舞 対 视频 mkt858 mkt 858 "),
                RowFactory.create(0, "威信 脱 裤 秀 看 小 皇片 你懂的 懂 "),
                RowFactory.create(1, "大 羽 宝贝 一被 壕 发现了 发现 就要 要被 带走了 带走 走了 ")
        ));
        StructType schema = new StructType(new StructField[]{
                new StructField("label", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("sentence", DataTypes.StringType, false, Metadata.empty())
        });
        DataFrame sentenceData = sqlContext.createDataFrame(jrdd, schema);
        Tokenizer tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words");
        DataFrame wordsData = tokenizer.transform(sentenceData);
        int numFeatures = 500000;
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures")
                .setNumFeatures(numFeatures);
        DataFrame featurizedData = hashingTF.transform(wordsData);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);
        DataFrame rescaledData = idfModel.transform(featurizedData);

        NaiveBayesModel loadedModel =     NaiveBayesModel.load(ctx.sc(),"D:\\文本分类\\\\model");



        for (Row r : rescaledData.select("features", "label").take(3)) {

            Vector features = r.getAs(0);
            Integer label =  r.getInt(1);
           // System.out.println(features);
            System.out.println(label);
            System.out.println(loadedModel.predict(features));
        }
    }
}
