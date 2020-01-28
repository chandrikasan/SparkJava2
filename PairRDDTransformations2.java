import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class PairRDDTransformations2 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Transformation - CombineByKey
        JavaRDD<String> RDD1 = sc.textFile("C:\\Users\\Chandrika Sanjay\\ExampleForCombineByKey.txt", 3);

        JavaPairRDD<String,Integer> RDD2 = RDD1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words = s.split("\t");
                String key = words[0];
                Integer value = Integer.parseInt(words[1]);
                Tuple2 t = new Tuple2(key, value);
                return t;
            }
        });

        System.out.println("RDD of Category,Clicks: ");
        System.out.println(RDD2.collect());

        Function<Integer,Tuple2<Integer,Integer>> createCombiner = new Function<Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) {
                return new Tuple2<>(i, 1);
            }
        };

        Function2<Tuple2<Integer,Integer>, Integer, Tuple2<Integer,Integer>> mergeValue = new Function2<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t, Integer i) {
                return new Tuple2<>(t._1 + i, t._2 + 1);
            }
        };

        Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>> mergeCombiners = new Function2<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> t1, Tuple2<Integer, Integer> t2) {
                return new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2);
            }
        };


        // Transformation - CombineByKey

        JavaPairRDD<String, Tuple2<Integer,Integer>> RDD3 = RDD2.combineByKey(createCombiner,mergeValue,mergeCombiners);

        System.out.println("RDD of Category and Total clicks in each category and Count in each category: ");
        System.out.println(RDD3.collect());


        System.out.println("Average clicks in each category:");
        RDD3.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Integer>> s) {
                String category = s._1;
                double sum = s._2._1.doubleValue();
                double count = s._2._2.doubleValue();
                System.out.printf("%s : %.2f \n ", category , (sum / count));
            }
        });


    }
}
