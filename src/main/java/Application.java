import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;

public class Application {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("TP 1 RDD").setMaster("local[*]");
        JavaSparkContext sc=new JavaSparkContext(conf);
        JavaRDD<String> rdd1=sc.parallelize(Arrays.asList("Arij","Foo","Raa","Saas","Pooo","Arm"));
        JavaRDD<String> rdd2=rdd1.flatMap(s->Arrays.asList(s.split(",")).iterator());
        JavaRDD<String> rdd3=rdd2.filter(s->s.contains("aa"));
        JavaRDD<String> rdd4=rdd2.filter(s->s.contains("oo"));
        JavaRDD<String> rdd5=rdd2.filter(s->s.length()==3);
        JavaRDD<String> rdd6=rdd3.union(rdd4);
        JavaRDD<String> rdd71=rdd5.map(s->s.concat("aas"));
        JavaRDD<String> rdd81=rdd6.map(s->s.concat("oop"));
        JavaPairRDD<String,Integer> rdd7=rdd71.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> rdd8=rdd81.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> rdd9=rdd7.union(rdd8);
        JavaPairRDD<String,Integer> rdd10=rdd9.sortByKey();

        rdd10.foreach(nameTuple-> System.out.println(nameTuple._1()+" "+nameTuple._2()));
    }
}
