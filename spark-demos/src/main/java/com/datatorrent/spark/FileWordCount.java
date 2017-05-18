package com.datatorrent.spark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class FileWordCount
{
  private static final Pattern SPACE = Pattern.compile(" ");

  public static void main(String[] args)
  {

    // Create the context with a 1 second batch size
    SparkConf sparkConf = new SparkConf().setAppName("Basic Word Count");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(15));
    JavaDStream<String> lines = ssc.textFileStream(args[0]);
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>()
    {
      private static final long serialVersionUID = -5196806959227239313L;

      public Iterable<String> call(String x)
      {
        return Arrays.asList(x.split(" "));
      }
    });

    JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>()
    {
      private static final long serialVersionUID = -5407238253923116164L;

      public Tuple2<String, Integer> call(String s)
      {
        return new Tuple2<String, Integer>(s, 1);
      }
    });

    JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>()
    {
      private static final long serialVersionUID = -3599666493519077859L;

      public Integer call(Integer i1, Integer i2)
      {
        return i1 + i2;
      }
    });
    
    //TODO write the counts to a file
    wordCounts.print(100);
    ssc.start(); // Start the computation
    ssc.awaitTermination();
  }
}
