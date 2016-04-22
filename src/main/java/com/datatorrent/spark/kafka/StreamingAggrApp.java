package com.datatorrent.spark.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Pattern;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.collect.Lists;

import com.datatorrent.spark.model.UserAggregate;
import com.datatorrent.spark.model.UserEvent;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount. Usage:
 * JavaDirectKafkaWordCount <brokers> <topics> <brokers> is a list of one or
 * more Kafka brokers <topics> is a list of one or more kafka topics to consume
 * from
 *
 * Example: $ bin/run-example streaming.JavaDirectKafkaWordCount
 * broker1-host:port,broker2-host:port \ topic1,topic2
 */

public final class StreamingAggrApp
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
    JavaDStream<UserAggregate> lines = messages.map(new Function<Tuple2<String, String>, UserAggregate>()
    {
      @Override
      public UserAggregate call(Tuple2<String, String> tuple2)
      {
        if (tuple2._2.length() < 2) {
          return null;
        }

        UserAggregate event = new UserAggregate();
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
        } else {
          event.setPrice(0.0);
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

        event.setCount(0L);
        event.setRevenue(0.0);

        return event;

      }
    });

    //TODO - convert the JavaDStream to javaPairDStream

    JavaPairDStream<String, UserAggregate> aggregateTuple = lines
        .flatMapToPair(new PairFlatMapFunction<UserAggregate, String, UserAggregate>()
        {

          /**
           * 
           */
          private static final long serialVersionUID = -8516588035496512322L;

          @Override
          public Iterable<Tuple2<String, UserAggregate>> call(UserAggregate rdd) throws Exception
          {
            ArrayList<Tuple2<String, UserAggregate>> arr = Lists.newArrayList();

            String key = "" + rdd.getId() + "," + rdd.getTime();
            rdd.setCount(1L);
            if (rdd.getPrice() != null && rdd.getPrice() > 0)
              rdd.setRevenue(rdd.getPrice());

            arr.add(new Tuple2<String, UserAggregate>(key, rdd));

            return arr;
          }

        });

    //TODO add a partitioner here

    JavaPairDStream<String, UserAggregate> reduced = aggregateTuple
        .reduceByKeyAndWindow(new Function2<UserAggregate, UserAggregate, UserAggregate>()
        {
          @Override
          public UserAggregate call(UserAggregate v1, UserAggregate v2) throws Exception
          {

            UserAggregate aggregate = new UserAggregate();
            aggregate.copy(v1);
            System.out.println("1-" + v1.toString());
            System.out.println("2-" + v2.toString());
            aggregate.setCount(v1.getCount() + v2.getCount());
            if (v1.getRevenue() != null && v2.getRevenue() != null)
              aggregate.setRevenue(v1.getRevenue() + v2.getRevenue());
            return aggregate;
          }
        }, Durations.seconds(30), Durations.seconds(10));

    reduced.foreachRDD(new VoidFunction<JavaPairRDD<String, UserAggregate>>()
    {

      /**
       * 
       */
      private static final long serialVersionUID = 3459375338033492394L;

      public void call(JavaPairRDD<String, UserAggregate> t) throws Exception
      {
        List<Tuple2<String, UserAggregate>> rddList = t.collect();
        for (int i = 0; i < rddList.size(); i++) {
          try {
            // if (results.get(i) != null)
            System.out.println("Aggregate is - " + rddList.get(i).toString());
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    });

    reduced.print();

    // Start the computation
    jssc.start();
    jssc.awaitTermination();
  }
}
