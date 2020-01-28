import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public class WordCountExample {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        JavaRDD<String> rdd1 = sc.textFile("C:\\users\\Chandrika Sanjay\\input1.txt");

        System.out.println(rdd1.collect());

        JavaRDD<String> wordsRDD = rdd1.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                String[] words = s.split(" ");
                ArrayList<String> strList = new ArrayList<>();
                strList.addAll(Arrays.asList(words));
                return strList.iterator();
            }
        });

        System.out.println(wordsRDD.collect());

        JavaPairRDD<String,Integer> countsRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2(s,1);
            }
        });

        countsRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> s) {
                System.out.println(s);
            }
        });

        JavaPairRDD<String,Integer> countsRDD2 = countsRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });

        System.out.println(countsRDD2.collect());
        //countsRDD2.saveAsTextFile("C:\\Users\\Chandrika Sanjay\\wordcount");


    }
}
