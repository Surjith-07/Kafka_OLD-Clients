package org.app;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Properties;

public class Producer {
    static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    static final String localhost = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, localhost);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"admin-secret\";");
//        System.setProperty("java.security.auth.login.config", "/home/surjith-pt7589/Current/kafka_2.12-3.7.0/kafka_2.12-3.7.0/config/kafka_client_jaas.conf");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        int i=0;
        while (true) {

            ProducerRecord<String, String> record = new ProducerRecord<>("test-plain", "key", "NEWsssss" + i++);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        LOGGER.info("\nRecivied Record Meta Data. \n" +
                                "Topic: " + recordMetadata.topic() + " , Partition " + recordMetadata.partition() + ", " +
                                "Offset: " + recordMetadata.offset() + ", TimeStamp: " + new Date(recordMetadata.timestamp()) + "\n");
//                        System.out.println(record);
                    } else {
                        LOGGER.error("ERROR OCCURED", e);

                    }
                }

            });
            System.out.println("New-Message " + i + " Pushed");
            LOGGER.info("New-Message {} Pushed", i);
            Thread.sleep(1000);
        }
    }
}
// SLF4J --> https://coderanch.com/t/777120/java/SLF-SLF-providers