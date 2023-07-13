package org.example;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
public class WordCount {
    private static void wordCount(String FileName) {

        // Set Spark configuration
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Word Counter");

        // Set Spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Load text file
        JavaRDD<String> inputFile = sparkContext.textFile(FileName);

        // Apply transformations
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        JavaPairRDD countData = wordsFromFile.mapToPair(t-> new Tuple2(t,1)).reduceByKey((x,y)-> (int) x + (int) y);

        // Save data
        countData.saveAsTextFile("Output_text");
    }

    public static void main (String[] args) {

        // Define text file dir
        String FileName = "src/docs/shakespeare.txt";

        // Call the function
        wordCount(FileName);
    }
}
