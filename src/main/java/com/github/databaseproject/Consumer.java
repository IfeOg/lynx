package com.github.databaseproject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	public static void main (String [] args){
		
		final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
		
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-project-application";
		String topic = "my_topic";
		
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		
		
		
		//Create Consumer
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList("my_topic"));
		
		String[] split;
		//poll for new data
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll((Duration.ofMillis(100)));
			
			
			
			for (ConsumerRecord<String, String> record : records) {
				
				
				int wines;
				int fruit;
				int meat;
				int fish;
				int sweet;
				int gold;
				String MaritalStatus;
				int id;
				
				
				for (int i = 0; i <= 5; i++) {
					
					
					split =  record.value().split(",");
					System.out.print(split[0] + " ");
					System.out.print(split[1] + " ");
					System.out.print(split[2] + " ");
					System.out.print(split[3] + " ");
					System.out.print(split[4] + "\n");
					id = Integer.valueOf(split[i]);
				}
				
				
				
				
				
				//if (record.value().contains("Single")) {
				//logger.info("Key: " + record.key() + ", Value: " +record.value());
				}
				//System.out.println("message received");
				
				//logger.info("Partition: " + record.partition() + ", Offset: " +record.offset());
			}
			
			
			
		}
}


