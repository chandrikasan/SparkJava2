import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.function.Consumer;

public class PairRDDExamples {
    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*************************************************************************************/
        //Example - 1 : Create Pair RDD of Integer,Integer

        JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8), 2);

        JavaPairRDD<Integer,Integer> pairRDD = rdd1.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer i) {
                return new Tuple2(i, i*i);
            }
        });

        System.out.println(pairRDD.collect());


        //Example - 2 : Create Pair RDD of String,String
        JavaRDD<String> fileRDD = sc.textFile("C:\\student.txt");

        System.out.println(fileRDD.collect());
        System.out.println(fileRDD.count());

        JavaRDD<String> splitFile = fileRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) {
                String[] words = s.split(" ");
                ArrayList<String> strList = new ArrayList<>();

                strList.addAll(Arrays.asList(words));
                return strList.iterator();
            }
        });

        System.out.println(splitFile.collect());

        JavaPairRDD<String,String> stringPairRDD = splitFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                return (new Tuple2(s, "WORD"));
            }
        });

        System.out.println(stringPairRDD.collect());

        //Example - 3 : Create Pair RDD of String,String

        JavaRDD<String> RDD1 = sc.textFile("C:\\Users\\Chandrika Sanjay\\Documents\\student2.txt");

        List<String> list1 =new ArrayList<>();

        JavaPairRDD<String,String> RDD2 = RDD1.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) {
                String[] words = s.split(":");
                String key = words[0];
                String value = words[1];
                Tuple2 t = new Tuple2(key, value);
                return t;
            }
        });
        System.out.println("Pair RDD created :");
        System.out.println(RDD2.collect());
        RDD2.collect().forEach(new Consumer<Tuple2<String, String>>() {
            @Override
            public void accept(Tuple2<String, String> s) {
                System.out.println(s);
            }
        });

        Scanner scanner = new Scanner(System.in);
        System.out.println("Enter q to quit:");
        String s = scanner.next();
    }

}
