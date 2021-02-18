package com.ladutsko.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FibonacciProducer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(FibonacciProducer.class);

        int fibonacciNumber = Integer.parseInt(args[0]);

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        if (fibonacciNumber > 0) {
            int firstNumber = 0;
            int secondNumber = 1;
            int sum;
            for (int i = 0; i < fibonacciNumber; i++) {
                ProducerRecord<String, String> record;
                if (i < 2) {
                    record = new ProducerRecord<>(Constants.TOPIC, Integer.toString(i));
                } else {
                    sum = firstNumber + secondNumber;
                    record = new ProducerRecord<>(Constants.TOPIC, Integer.toString(sum));
                    firstNumber = secondNumber;
                    secondNumber = sum;
                }

                producer.send(record, (recordMetadata, e) -> {
                    if (e == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n");
                    } else {
                        logger.error("Error while producing: ", e);
                    }
                });
            }
        }

        producer.flush();
        producer.close();

    }

}
