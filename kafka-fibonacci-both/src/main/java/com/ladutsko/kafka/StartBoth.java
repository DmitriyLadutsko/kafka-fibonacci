package com.ladutsko.kafka;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class StartBoth {

    private final static String TOPIC = "fibonacci";
    private final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private final static String OFFSET_RESET_RULE = "earliest";


    private static class Producer extends Thread {

        private int fibonacciNumber;
        private Properties props = getProducerProps();

        Producer(int fibonacciNumber) {
            this.fibonacciNumber = fibonacciNumber;
        }

        @Override
        public void run() {


            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            if (fibonacciNumber > 0) {
                int firstNumber = 0;
                int secondNumber = 1;
                int sum;
                try {

                    Thread.sleep(50);

                    for (int i = 0; i < fibonacciNumber; i++) {
                        ProducerRecord<String, String> record;
                        if (i < 2) {
                            record = new ProducerRecord<>(TOPIC, Integer.toString(i));
                        } else {
                            sum = firstNumber + secondNumber;
                            record = new ProducerRecord<>(TOPIC, Integer.toString(sum));
                            firstNumber = secondNumber;
                            secondNumber = sum;
                        }

                        producer.send(record);
                    }
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }

            producer.flush();
            producer.close();

        }
    }

    private static class Consumer extends Thread {

        private int numberMessagesToRead;

        private Properties props = getConsumerProps();

        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        public Consumer(int numberMessagesToRead) {
            this.numberMessagesToRead = numberMessagesToRead;
        }

        @Override
        public void run() {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

            consumer.subscribe(Collections.singleton(TOPIC));

            int numberMessagesReadSoFar = 0;
            int sumFibonacci = 0;

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

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

    private static Properties getProducerProps() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_RULE);
        return properties;
    }

    public static void main(String[] args) {

        Producer producer = new Producer(Integer.parseInt(args[0]));
        Consumer consumer = new Consumer(Integer.parseInt(args[1]));
        producer.start();
        consumer.start();

    }
}
