package org.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class TextSearch {
    
    public static void main (String[] args){

        // Set log level to warning
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        
        // Set Spark configuration
        SparkConf conf = new SparkConf().setAppName("Text search").setMaster("local[*]");

        // Set Spark context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Set Spark session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Creates a DataFrame having a single column named "line"
        JavaRDD<String> textFile = sc.textFile("src/log.log");
        JavaRDD<Row> rowRDD = textFile.map(RowFactory::create);
        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("line", DataTypes.StringType, true));
        StructType schema = DataTypes.createStructType(fields);
        Dataset <Row> df = spark.createDataFrame(rowRDD, schema);
        Dataset <Row> errors = df.filter(col("line").like("%ERROR%"));

        // Counts all the errors
        System.out.println("This is total errors: "+ errors.count());

        // Counts errors mentioning MySQL
        System.out.println("This is total errors mentioning MySQL: "+ errors.filter(col("line").like("%MySQL%")).count());

        // Fetches the MySQL errors as an array of strings
        errors.filter(col("line").like("%MySQL%")).collect();
    }
}
