package com.ladutso.kafka.group;

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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerGroupApplication {

    private final static String TOPIC = "kafka_group";
    private final static String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private final static String GROUP_ID = "group_application";
    private final static String OFFSET_RESET_RULE = "earliest";

    private final static String firstConsumerOut = "first_consumer.txt";
    private final static String secondConsumerOut = "second_consumer.txt";

    private static int consumersNumber = 0;

    private static class Producer extends Thread {

        private int messagesNumber;
        private Properties props = getProducerProps();

        Producer(int messagesNumber) {
            this.messagesNumber = messagesNumber;
        }

        @Override
        public void run() {

            KafkaProducer<String, String> producer = new KafkaProducer<>(props);

            try {
                for (int i = 1; i <= messagesNumber; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, (i) + "kafka");

                    Thread.sleep(100);
                    System.out.println(i);
                    producer.send(record);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }

            producer.flush();
            producer.close();

        }
    }

    private static class Consumer extends Thread {

        private String consumerName;
        private String fileToWrite;
        private int consumerRate;

        private Properties props = getConsumerProps();

        final Logger logger = LoggerFactory.getLogger(Consumer.class);

        Consumer(String consumerName, String fileToWrite) {
            this.consumerName = consumerName;
            this.fileToWrite = fileToWrite;
            this.consumerRate = consumersNumber;
            consumersNumber++;
        }

        @Override
        public void run() {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            System.out.println("====> [consumersNumber]: " + consumerRate);

            consumer.subscribe(Collections.singleton(TOPIC));

            if (consumerRate > 0) {
                try {
                    Thread.sleep(20000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Consumer: " + consumerName +
                            " Value: " + record.value() +
                            " Partition: " + record.partition());
                }
                if (!records.isEmpty()) {
                    try {
                        saveRecords(records);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }

            }
        }

        private void saveRecords(ConsumerRecords<String, String> records) throws IOException {
            FileWriter fileWriter = new FileWriter(new File(fileToWrite), true);
            for (ConsumerRecord<String, String> record : records) {
                String recordToFile = String.format("%s, %s, %s", consumerName, record.value(), record.partition());
                fileWriter.write(recordToFile + System.getProperty("line.separator"));
            }

            fileWriter.close();

        }
    }

    private static Properties getProducerProps() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "100");
        return properties;
    }

    private static Properties getConsumerProps() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_RULE);
        return properties;
    }

    public static void main(String[] args) {
        Producer producer = new Producer(Integer.parseInt(args[0]));
        Consumer firstConsumer = new Consumer(args[1], firstConsumerOut);
        Consumer secondConsumer = new Consumer(args[2], secondConsumerOut);
        producer.start();
        firstConsumer.start();
        secondConsumer.start();
    }

}
