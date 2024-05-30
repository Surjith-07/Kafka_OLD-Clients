package org.app;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class Consumer {
    static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    static final String bootstrapServer = "localhost:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Consumer-group-0");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        properties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        properties.setProperty("sasl.mechanism", "PLAIN");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"client\" password=\"client123\";");
//        System.setProperty("java.security.auth.login.config", "/home/surjith-pt7589/Current/kafka_2.12-3.7.0/kafka_2.12-3.7.0/config/kafka_client_jaas.conf");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(List.of("sasl-plaintext"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(2);
            for (ConsumerRecord<String, String> record : records) {
                LOGGER.info("\nRecieved record: \n" +
                        "Key: " + record.key() + ", " +
                        "Value: " + record.value() + ", " +
                        "Topic: " + record.topic() + ", " +
                        "Partition: " + record.partition() + ", " +
                        "Offset: " + record.offset() + "\n");
                System.out.println("Consumed Offset: " + record.offset() + ", Message : " + record.value());
            }
        }
    }
}
