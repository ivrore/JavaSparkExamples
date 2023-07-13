package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

public class AgeCountMysql {

    public static void main (String[] args) {

        // Set log level to warning
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        // Set Spark configuration
        SparkConf conf = new SparkConf().setAppName("Text search").setMaster("local[*]");

        // Set Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Set Spark session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Creates a DataFrame based on a table named "people"
        // stored in a MySQL database.
        String url = "jdbc:mysql://host:port/db?user=myusername&password=mypassword";
        Dataset<Row> df = spark
                .read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "tablename")
                .load();

        // Looks the schema of this DataFrame.
        df.printSchema();

        // Counts people by age
        Dataset<Row> countsByAge = df.groupBy("age").count();
        countsByAge.show();

        // Saves countsByAge in JSON format.
        countsByAge.write().format("json").save("src/counts_by_age");

        // Creates count_age table
        countsByAge.write().format("json").saveAsTable("Count_age");

        // Test query to see if table is created
        spark.sql("select * from Count_age limit 2").show();
    }
}
