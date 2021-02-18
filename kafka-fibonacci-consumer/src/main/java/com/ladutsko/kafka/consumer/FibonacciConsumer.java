package com.ladutsko.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FibonacciConsumer {

    public static void main(String[] args) {


        Logger logger = LoggerFactory.getLogger(FibonacciConsumer.class);

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Constants.GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Constants.OFFSET_RESET_RULE);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton(Constants.TOPIC));

        int numberMessagesToRead = Integer.parseInt(args[0]);
        int numberMessagesReadSoFar = 0;
        int sumFibonacci = 0;

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberMessagesReadSoFar += 1;
                if (numberMessagesReadSoFar <= numberMessagesToRead) {
                    sumFibonacci += Integer.parseInt(record.value());
                }
                logger.info("Value" + numberMessagesReadSoFar + ": " + record.value());
            }
            if (!records.isEmpty()) {
                logger.info("The sum of " + numberMessagesToRead + " Fibonacci numbers is " + sumFibonacci);
                numberMessagesReadSoFar = 0;
                sumFibonacci = 0;
            }

        }

    }

}
