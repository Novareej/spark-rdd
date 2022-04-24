import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Application2 {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("ventes").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.textFile("ventes.txt");
        JavaRDD<String> rdd2=rdd1.flatMap(s-> Arrays.asList(s.split("\n")).iterator());
        //Pair de ville & prix
        JavaPairRDD<String,Double> rdd3= rdd2.mapToPair(s->new Tuple2<>(s.split(" ")[1],Double.parseDouble(s.split(" ")[3])));
        // Somme des ventes
        JavaPairRDD<String,Double> rdd4 = rdd3.reduceByKey((v1,v2)->v1+v2);
        rdd4.foreach(a-> System.out.println(a));
    }
}
