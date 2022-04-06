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

	public static void main(String[] args) {

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

		// Create Consumer

		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		consumer.subscribe(Arrays.asList("my_topic"));

		// poll for new data
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll((Duration.ofMillis(100)));

			for (ConsumerRecord<String, String> record : records) {

				String[] split;
				String maritalStatus;
				String education;
				int MntWines = 0;
				int MntFruits = 0;
				int MntMeatProducts = 0;
				int MntFishProducts = 0;
				int MntSweetProducts = 0;
				int MntGoldProds = 0;
				double income = 0;
				int id;
				int year;

				split = record.value().split(",");

				id = Integer.valueOf(split[0]);
				maritalStatus = split[1];
				education = split[2];
				
				if(split[3] == "") {
					income = 0;
				} else {
					income = Integer.valueOf(split[3]);
				}
				
				year = Integer.valueOf(split[4]);
				MntWines = Integer.valueOf(split[5]);
				MntFruits = Integer.valueOf(split[6]);
				MntMeatProducts = Integer.valueOf(split[7]);
				MntFishProducts = Integer.valueOf(split[8]);
				MntSweetProducts = Integer.valueOf(split[9]);
				MntGoldProds = Integer.valueOf(split[10]);
				
				System.out.println("id: " + id + ". marital status: "+ maritalStatus + ". education: " + education + ". mnt fish: " + MntFishProducts);
				System.out.println("mnt meat: " + MntMeatProducts + ". mnt sweet: " + MntSweetProducts + ". mnt gold " + MntGoldProds);
				System.out.println("id: " + id);

				double totalSpent = (MntFishProducts + MntFruits + MntGoldProds + MntMeatProducts + MntSweetProducts
						+ MntWines);
				
				System.out.println("income: " + income);
				System.out.println("total spent : " + totalSpent);

				double atRisk = 0;

				if (income == 0) {
					atRisk = 0;
				} else {
					atRisk = totalSpent / income * 2;
				}

				System.out.println("At risk = " + atRisk);

				// if (record.value().contains("Single")) {
				// logger.info("Key: " + record.key() + ", Value: " +record.value());
			}
			// System.out.println("message received");

			// logger.info("Partition: " + record.partition() + ", Offset: "
			// +record.offset());
		}

	}
}
