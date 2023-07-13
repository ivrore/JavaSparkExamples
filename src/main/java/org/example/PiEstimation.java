package org.example;

import java.util.ArrayList;
import java.util.List;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

// Set log level
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


public class PiEstimation {
    private static final int NUM_SAMPLES = 100000;
    public static void main(String[] args) {
        List<Integer> l = new ArrayList<>(NUM_SAMPLES);
        for (int i = 0; i < NUM_SAMPLES; i++) {
            l.add(i);
        }
        // Set log level to warning
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Set spark configuration and spark context
        SparkConf conf = new SparkConf().setAppName("Pi Estimation").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        long count = sc.parallelize(l).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x * x + y * y < 1;
        }).count();
        System.out.println("Pi is roughly " + 4.0 * count / NUM_SAMPLES);
    }
}

