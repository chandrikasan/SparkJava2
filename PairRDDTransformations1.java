import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class PairRDDTransformations1 {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> RDD3 = sc.parallelize(Arrays.asList("aaa,10" ,"bbb,20", "aaa,20" ,
                "ccc,30" ,"eee,50" , "ddd,40" ,"bbb,60" ,"ccc,80" , "eee,100"));

        JavaPairRDD<String,Integer> RDD4 = RDD3.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) {
                String[] words = s.split(",");
                String key = words[0];
                Integer value = Integer.parseInt(words[1]);
                Tuple2 t = new Tuple2(key, value);
                return t;
            }
        });


        System.out.println("RDD4 (RDD of string,integer):");
        System.out.println(RDD4.collect());

        // Transformation - mapValues
        JavaPairRDD RDD6 = RDD4.mapValues(x -> (x + 100));
        System.out.println("Output of mapValues:");
        System.out.println(RDD6.collect());

        // Transformation - flatMapValues
        System.out.println("Output of flatMapValues:");
        JavaPairRDD<String,Integer> flatRDD = RDD4.flatMapValues(new Function<Integer, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(Integer x) {
                return Arrays.asList(x+1,x+2,x+3);
            }
        });

        System.out.println(flatRDD.collect());

        //Transformation - Filter
        JavaPairRDD filteredRDD4 = RDD4.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> t) {
                //System.out.println("Tuple elements: " + t._1 + " " + t._2);
                return (t._2 <= 50);
            }
        });
        System.out.println("Filtered RDD4 to select elements having values less than or equal to 50");
        System.out.println(filteredRDD4.collect());


        //Transformation - Keys
        System.out.println("RDD4.keys:");
        System.out.println(RDD4.keys().collect());

        //Transformation - Values
        System.out.println("RDD4.values:");
        System.out.println(RDD4.values().collect());

        //Transformation - GroupByKey
        System.out.println("Output of group by key:");
        System.out.println(RDD4.groupByKey().collect());


        //Transformation - ReduceByKey
        JavaPairRDD RDD5 = RDD4.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer x, Integer y) {
                return x + y;
            }
        });
        System.out.println("Output of reduce by Key:");
        System.out.println(RDD5.collect());


        //Transformation - SortByKey
        System.out.println("Output of sort by key:");
        System.out.println(RDD4.sortByKey().collect());


    }
}
