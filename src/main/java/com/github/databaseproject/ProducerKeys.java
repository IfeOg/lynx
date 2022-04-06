package com.github.databaseproject;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerKeys {

	public static void main (String [] args){
		
		final Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class.getName());
		//create producer properties
		String bootstrapServers = "127.0.0.1:9092";
		Properties properties= new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		//Creating the producer
		
		KafkaProducer<String, String> producer= new KafkaProducer <String, String>(properties);
		
		for (int i=0; i<10; i++) {
		//Create a producer record
		ProducerRecord <String, String> record = new ProducerRecord<String, String> ("my_topic", "hello world");
		
		//Send data
		producer.send(record, new Callback() {
			public void onCompletion(RecordMetadata recordMetadata, Exception e) {
				 //executes every time a record successfully sends
			if (e != null) {
				//record was successfully sent
			
				logger.info("Received new metadata. \n" +
			    "Topic:" +recordMetadata.topic() + "\n" +
			    "Partition:" +recordMetadata.partition() + "\n" +
			    "Offset:" +recordMetadata.offset() + "\n" +
			    "Timestamp:" +recordMetadata.timestamp());
			} else {
				logger.error("Error while producing", e);
			}
				
				
				
			}});
		
		
		}
		
		
		//flush data
		producer.flush();
		producer.close();
		
	}
}