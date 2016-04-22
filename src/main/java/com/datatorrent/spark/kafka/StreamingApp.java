package com.datatorrent.spark.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import com.datatorrent.spark.model.UserEvent;

import org.apache.spark.streaming.Durations;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class StreamingApp
{
  private static final Pattern SPACE = Pattern.compile(" ");

  @SuppressWarnings("deprecation")
  public static void main(String[] args)
  {
    if (args.length < 2) {
      System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
          + "  <brokers> is a list of one or more Kafka brokers\n"
          + "  <topics> is a list of one or more kafka topics to consume from\n\n");
      System.exit(1);
    }

    String brokers = args[0];
    String topics = args[1];

    // Create context with a 2 seconds batch interval
    SparkConf sparkConf = new SparkConf().setAppName("JavaDirectKafkaWordCount");
    JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

    HashSet<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
    HashMap<String, String> kafkaParams = new HashMap<>();
    kafkaParams.put("metadata.broker.list", brokers);

    //TODO - Read an external file for enrichment

    // Create direct kafka stream with brokers and topics
    JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
        StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

    // Get the lines, split them into words, count the words and print
    JavaDStream<UserEvent> lines = messages.map(new Function<Tuple2<String, String>, UserEvent>()
    {
      @Override
      public UserEvent call(Tuple2<String, String> tuple2)
      {
        if (tuple2._2.length() < 2) {
          return null;
        }

        UserEvent event = new UserEvent();
        JSONParser parser = new JSONParser();
        JSONObject tuple = null;

        try {
          tuple = (JSONObject)parser.parse(tuple2._2());
        } catch (ParseException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        if (tuple.containsKey("id")) {
          event.setId((Long)tuple.get("id"));
        }

        if (tuple.containsKey("name")) {
          event.setName((String)tuple.get("name"));
        }

        if (tuple.containsKey("price")) {
          event.setPrice((Double)tuple.get("price"));
        }

        if (tuple.containsKey("tags")) {
          JSONArray arr = (JSONArray)tuple.get("tags");

          if (event.getTags() == null) {
            event.setTags(new ArrayList<String>());
          }
          for (int i = 0; i < arr.size(); i++) {
            event.getTags().add(arr.get(i).toString());
          }

        }

        if (tuple.containsKey("time")) {
          event.setTime((Long)tuple.get("time"));
        }

        return event;

      }
    });

    //TODO - convert the JavaDStream to javaPairDStream

    
    
    JavaPairDStream<String, Integer> aggregateTuple = lines.flatMapToPair(new PairFlatMapFunction<UserEvent, String, Integer>()
    {

      /**
       * 
       */ 
      private static final long serialVersionUID = -8516588035496512322L;

      @Override
      public Iterable<Tuple2<String, Integer>> call(UserEvent rdd) throws Exception
      {
        ArrayList<Tuple2<String, Integer>> arr = Lists.newArrayList();

        String key = "" + rdd.getId() + rdd.getTime();
        arr.add(new Tuple2<String, Integer>(key, 1));

        return arr;
      }

    });

    //TODO add a partitioner here
    
    JavaPairDStream <String,Integer> reduced = aggregateTuple.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>()
    {
      @Override
      public Integer call(Integer v1, Integer v2) throws Exception
      {
        return v1+v2; 
      }
    }, Durations.seconds(30), Durations.seconds(10));

    //    lines.foreachRDD(new Function<JavaRDD<UserEvent>, Void>()
    //    {
    //      /**
    //       * 
    //       */
    //      private static final long serialVersionUID = 1L;
    //
    //      @Override
    //      public Void call(JavaRDD<UserEvent> rdd) throws Exception
    //      {
    //        if (rdd != null) {
    //          List<UserEvent> results = rdd.collect();
    //          for (int i = 0; i < results.size(); i++) {
    //            UserEvent userEvent = results.get(i);
    //            System.out.println("Result is - " + userEvent.toString());
    //          }
    //        }
    //        return null;
    //      }
    //    });
    
    reduced.foreachRDD(new VoidFunction<JavaPairRDD<String,Integer>>()
    {
      
      public void call(JavaPairRDD<String, Integer> t) throws Exception
      {
        List<Tuple2<String, Integer>> rddList = t.collect();
        
        for (int i = 0; i < rddList.size(); i++) {
          try {
           // if (results.get(i) != null)
              System.out.println("Aggregate is - " + rddList.get(i));
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
        
      }
    });  
    
    reduced.print();

    //   lines.print();

    //    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>()
    //    {
    //      public Iterable<String> call(String x)
    //      {
    //        return Lists.newArrayList(SPACE.split(x));
    //      }
    //    });
    //
    //    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(new PairFunction<String, String, Integer>()
    //    {
    //      @Override
    //      public Tuple2<String, Integer> call(String s)
    //      {
    //        return new Tuple2<>(s, 1);
    //      }
    //    }).reduceByKey(new Function2<Integer, Integer, Integer>()
    //    {
    //      @Override
    //      public Integer call(Integer i1, Integer i2)
    //      {
    //        return i1 + i2;
    //      }
    //    });
    //   wordCounts.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
