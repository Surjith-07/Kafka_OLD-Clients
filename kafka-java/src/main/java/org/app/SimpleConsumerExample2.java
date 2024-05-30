package org.app;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;

public class SimpleConsumerExample2 {
    static final Logger LOGGER = LoggerFactory.getLogger(SimpleConsumerExample2.class);
    static final String topic = "sasl-plaintext";
    static final int partition = 1;
    static final List<String> brokers = List.of("localhost:9092", "localhost:9093", "localhost:9094");
    static final int fetchSize = 1024;
    static final int soTimeout = 10000;
    static final String clientId = "1234";
    static final int maxRetries = 5;

    public static void main(String[] args) {
        fetchMessagesFromBrokers(0);
//        fetchMessagesFromBrokers(1);
//        fetchMessagesFromBrokers(2);
    }

    private static void fetchMessagesFromBrokers(int brokerIndex) {
        if (brokerIndex >= brokers.size()) {
            brokerIndex = 0;
        }

        String broker = brokers.get(brokerIndex);
        fetchMessages(broker);
    }

    private static void fetchMessages(String broker) {
        String[] hostPort = broker.split(":");
        String brokerHost = hostPort[0];
        int brokerPort = Integer.parseInt(hostPort[1]);

        long offset = 0;
        int numErrors = 0;

        while (true) {
            SimpleConsumer consumer = new SimpleConsumer(brokerHost, brokerPort, soTimeout, fetchSize, clientId);

            FetchRequest request = new FetchRequestBuilder()
                    .clientId(clientId)
                    .addFetch(topic, partition, offset, fetchSize)
                    .build();
            FetchResponse response = null;

            try {
                response = consumer.fetch(request);
            } catch (Exception e) {
                LOGGER.error("Error fetching data from broker {}:{} Reason: ", brokerHost, brokerPort, e);
                if (numErrors++ > maxRetries) {
                    consumer.close();
                    return;
                }
                continue;
            }

            if (response.hasError()) {
                short code = response.errorCode(topic, partition);
                LOGGER.error("Error fetching data from broker {}:{} Reason: {}", brokerHost, brokerPort, code);
                if (numErrors++ > maxRetries) {
                    consumer.close();
                    return;
                }
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    offset = 0;
                }
                continue;
            }

            numErrors = 0;
            ByteBufferMessageSet messageSet = response.messageSet(topic, partition);
            for (MessageAndOffset messageAndOffset : messageSet) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < offset) {
                    LOGGER.info("Found an old offset: " + currentOffset + " Expecting: " + offset);
                    continue;
                }

                offset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);
                String message = new String(bytes);

                LOGGER.info("Received message: " + message + " Offset: " + messageAndOffset.offset());
                System.out.println("Consumed Offset: " + messageAndOffset.offset() + ", Message: " + message);
            }

            consumer.close();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Consumer interrupted", e);
            }
        }
    }
}
