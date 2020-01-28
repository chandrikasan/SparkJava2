import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PairRDDTransformations3 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // Joins
        List<Tuple2<String,Integer>> list1 = new ArrayList<>();
        list1.add(new Tuple2("Uttar Pradesh",229));
        list1.add(new Tuple2("Maharashtra",121));
        list1.add(new Tuple2("Bihar",119));
        list1.add(new Tuple2("West Bengal",98));
        list1.add(new Tuple2("Madhya Pradesh",82));
        list1.add(new Tuple2("Rajasthan",78));
        list1.add(new Tuple2("Tamil Nadu",76));


        List<Tuple2<String, Integer>> list2 = Arrays.asList(
                new Tuple2("Madhya Pradesh",10),
                new Tuple2("Rajasthan",20),
                new Tuple2("Tamil Nadu",30),
                new Tuple2("Karnataka",40),
                new Tuple2("Gujarat",50),
                new Tuple2("Andhra Pradesh",60));

        JavaPairRDD<String,Integer> RDD1 = sc.parallelizePairs(list1);
        JavaPairRDD<String,Integer> RDD2 = sc.parallelizePairs(list2);
        System.out.println("Sorted RDD1:");
        System.out.println(RDD1.sortByKey().collect());
        System.out.println("RDD1: ");
        System.out.println(RDD1.collect());
        System.out.println("RDD2: ");
        System.out.println(RDD2.collect());

        // Transformation - SubtractByKey
        System.out.println("RDD1 subtract by key RDD2: ");
        System.out.println(RDD1.subtractByKey(RDD2).collect());

        // Transformation - InnerJoin
        System.out.println("RDD1 join RDD2:");
        System.out.println(RDD1.join(RDD2).collect());

        // Transformation - LeftOuterJoin
        System.out.println("RDD1 LeftOuterJoin RDD2:");
        System.out.println(RDD1.leftOuterJoin(RDD2).collect());


        RDD1.leftOuterJoin(RDD2).foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Optional<Integer>>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Optional<Integer>>> s) {
                System.out.println(s._1 + " " + s._2._1 + " " + s._2._2.orElse(0));
            }
        });

        // Transformation - RightOuterJoin
        System.out.println("RDD1 RightOuterJoin RDD2:");
        System.out.println(RDD1.rightOuterJoin(RDD2).collect());

        RDD1.rightOuterJoin(RDD2).foreach(new VoidFunction<Tuple2<String, Tuple2<Optional<Integer>, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Optional<Integer>, Integer>> s) {
                System.out.println(s._1 + " " + s._2._1.orElse(0) + " " +  s._2._2);
            }
        });

        // Transformation - CoGroup
        System.out.println("RDD1 cogroup RDD2:");
        System.out.println(RDD1.cogroup(RDD2).collect());

        // Transformation - FullOuterJoin
        System.out.println("RDD1 fullOuterJoin RDD2: ");
        System.out.println(RDD1.fullOuterJoin(RDD2).collect());

    }
}
