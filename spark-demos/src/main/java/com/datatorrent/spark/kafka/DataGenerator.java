package com.datatorrent.spark.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class DataGenerator {
	
	public static void main(String[] args) {
		
		String brokers = args[0];
		int batchSize = Integer.parseInt(args[1]);
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer<String, String> producer = new KafkaProducer(props);
		
		System.out.println("Starting run");
		for(int i = 0; i < batchSize; i++) {
			String str = "" + System.currentTimeMillis();
			producer.send(new ProducerRecord<String, String>("input", str));
		}
		producer.close();
		System.out.println("Ending run");
	}
}
