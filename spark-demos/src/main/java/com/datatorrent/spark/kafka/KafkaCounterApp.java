package com.datatorrent.spark.kafka;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public final class KafkaCounterApp {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaCounterApp.class);

	public static long globalCounter = 0;
	public static long prevCount = 0;
	public static long currentRun = 0;
	public static long prev = 0L;
	public static long current = 0L;
	public static int batchCount = 0;
	public static Properties props = new Properties();

	public static void main(String[] args) {
		if (args.length < 6) {
			System.err.println(
					"Usage: KafkaCounterApp <brokers> <inputTopic> <outputTopic> <batchCount> <checkpointDirecroty> <windowDuration> <apexEventsVal>\n"
							+ "  <brokers> is a list of one or more Kafka brokers\n"
							+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		final String brokers = args[0];
		final String inputTopic = args[1];
		final String outputTopic = args[2];
		batchCount = Integer.parseInt(args[3]);
		final String checkpointDirectory = args[4];
		final int windowDuration = Integer.parseInt(args[5]);
		globalCounter = Long.parseLong(args[6]);

		props.put("bootstrap.servers", brokers);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", batchCount);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("KafkaCounterApp");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(windowDuration));

		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(inputTopic.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("auto.offset.reset", "largest");

		// Create direct kafka stream with brokers and topics
		JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		prev = System.currentTimeMillis();
		LOG.info("Starting run {}", prev);
		JavaDStream<String> streams = messages.map(new Function<Tuple2<String, String>, String>() {
			@Override
			public String call(Tuple2<String, String> tuple2) {
				return tuple2._2();
			}
		});

		streams.countByValueAndWindow(Durations.milliseconds(windowDuration), Durations.milliseconds(windowDuration))
				.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
					@Override
					public void call(JavaPairRDD<String, Long> t) throws Exception {

						List<Tuple2<String, Long>> rddList = t.collect();
						for (int i = 0; i < rddList.size(); i++) {
							try {
								globalCounter += rddList.get(i)._2();
								if ((globalCounter / batchCount) > (prevCount / batchCount)) {
									current = System.currentTimeMillis();
									LOG.info("First batch complete {}", globalCounter);
									emitOutput(props, globalCounter / batchCount, prev, current - prev, outputTopic);
									prev = current;
									prevCount = globalCounter;
								}
							} catch (Exception e) {
								LOG.error("Error in aggregating values", e);
							}
						}
					}
				});
		jssc.checkpoint(checkpointDirectory);
		jssc.start();
		jssc.awaitTermination();
	}

	public static void emitOutput(Properties props, long count, long startTime, long latency, String topic) {
		String str = "2," + startTime + "," + count + "," + latency;
		KafkaProducer producer = new KafkaProducer<>(props);
		producer.send(new ProducerRecord<String, String>(topic, str));
		producer.close();
	}
}