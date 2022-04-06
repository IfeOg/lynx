package com.github.databaseproject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class SqliteTest {
	
	public static void main(String[] args ) {
		String jdbcUrl = "jdbc:sqlite:/C:\\sqlite\\Test.db";
		try {
			Connection connection = DriverManager.getConnection(jdbcUrl);
			String sql = "SELECT * FROM test";
			Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery(sql);
			
			while (result.next()) {
				String crypto = result.getString("ID");
				
				String bootstrapServers = "127.0.0.1:9092";
				Properties properties= new Properties();
				properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
				properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
				
				
				//Creating the producer
				
				KafkaProducer<String, String> producer= new KafkaProducer <String, String>(properties);
				
				//Create a producer record
				ProducerRecord <String, String> record = new ProducerRecord<String, String> ("my_topic", crypto);
				System.out.println(crypto);
				
				//Send data
				producer.send(record);
				
				//flush data
				producer.flush();
				producer.close();
				
				
			}
			
		} catch (SQLException e) {
			System.out.println("Error connecting to SQLite database");
			e.printStackTrace();
		}
				
	}

}
