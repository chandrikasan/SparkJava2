import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

public class PairRDDActions {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> RDD3 = sparkContext.parallelize(Arrays.asList("aaa,30", "bbb,100", "aaa,20",
                "ccc,30", "eee,50", "ddd,40", "bbb,60", "ccc,80", "eee,100"));


        JavaPairRDD<String, Integer> RDD4 = RDD3.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] words = s.split(",");
                String key = words[0];
                Integer value = Integer.parseInt(words[1]);
                Tuple2 t = new Tuple2(key, value);
                return t;
            }
        });
        System.out.println(RDD4.collect());

        System.out.println("Count by key:");
        System.out.println(RDD4.countByKey());

        System.out.println("Collect as Map");
        Map<String,Integer> m1 = RDD4.collectAsMap();


        System.out.println(m1);

        System.out.println(RDD4.lookup("bbb"));



    }
}
